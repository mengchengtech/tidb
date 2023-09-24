// add by zhangbing

package executor_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech"

	// 强制调用preps包里的init方法
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/mock"
	_ "github.com/pingcap/tidb/mctech/preps"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type mctechStmtCases struct {
	source  string
	expect  string
	failure string
}

// func TestMain(t *testing.M) {

// }

func TestMCTechStatementsSummary(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []*mctechStmtCases{
		{"mctech select * from information_schema.statements_summary", "0  |none test 18446744073709551615|{}|SELECT * FROM `information_schema`.`statements_summary`", ""},
	}

	for _, c := range cases {
		tk.MustQuery(c.source).Check(
			testkit.RowsWithSep("|", c.expect))
	}
}

func TestForbiddenPrepare(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": true}),
	)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []*mctechStmtCases{
		{`prepare st from "select * from information_schema.statements_summary"`, "", "[mctech] PREPARE not allowed"},
	}

	for _, c := range cases {
		tk.MustContainErrMsg(c.source, c.failure)
	}
	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}

func TestIntegerAutoIncrement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := initMock(t, store)
	// Check for warning in case we can't set the auto_increment to the desired value
	tk.MustExec("create table t(a bigint primary key auto_increment)")
	res := tk.MustQuery("SHOW COLUMNS FROM t")

	lst := []string{}
	for _, row := range res.Rows() {
		lst = append(lst, fmt.Sprintf("%v", row))
	}

	require.Equal(t,
		strings.Join([]string{
			"[a bigint(20) NO PRI <nil> auto_increment]",
		}, "\n"),
		strings.Join(lst, "\n"),
		"TestIntegerAutoIncrement",
	)
}

func TestPrepareByQuery(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": false, "Tenant.Enabled": true}),
	)
	store := testkit.CreateMockStore(t)
	tk, sql := initDbAndData(t, store)

	session := tk.Session()
	var ctx context.Context
	ctx, _, _ = mctech.WithNewContext3(context.Background(), session, true)
	tk.MustExecWithContext(
		ctx,
		fmt.Sprintf(`prepare st from "%s"`, sql),
	)

	tk.MustExec(
		"SET @p1 = 'termination', @p2 = 'finished', @p3 = 'none', @p4 = 'project'",
	)

	var err error
	ctx, _, err = mctech.WithNewContext(session)
	require.NoError(t, err)

	rs := tk.MustQueryWithContext(
		ctx,
		`/*& tenant:mctest */ EXECUTE st USING @p1, @p2, @p3, @p4, @p4`,
	)

	rows1 := rs.Rows()
	seqs1 := map[string]any{}
	require.Len(t, rows1, 2)
	for _, row := range rows1 {
		seqs1[row[0].(string)] = true
	}

	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}

func TestPrepareByCmd(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": false, "Tenant.Enabled": true}),
	)

	store := testkit.CreateMockStore(t)
	tk, sql := initDbAndData(t, store)

	session := tk.Session()
	var ctx context.Context
	ctx, _, _ = mctech.WithNewContext3(context.Background(), session, true)
	result1 := tk.MustQueryWithContext(ctx, sql, "termination", "finished", "none", "project", "project", "mctest")

	rows1 := result1.Rows()
	seqs1 := map[string]any{}
	require.Len(t, rows1, 2)
	for _, row := range rows1 {
		seqs1[row[0].(string)] = true
	}
	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}

func TestPrepareByCmdNoTenant(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": false}),
	)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("select * from information_schema.statements_summary limit ?", 5)
	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_platform")
	tk.MustExec("create database global_platform")
	tk.MustExec("use global_platform")
	s := tk.Session()
	s.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}
	return tk
}

func initDbAndData(t *testing.T, store kv.Storage) (*testkit.TestKit, string) {
	tk := initMock(t, store)

	var createTableSQL0 = strings.Join([]string{
		"create table org_relation (",
		"tenant varchar(50),",
		"org_id bigint,",
		"child_org_id bigint,",
		"primary key(tenant, org_id, child_org_id)",
		")",
	}, "\n")
	var createTableSQL1 = strings.Join([]string{
		"create table project (",
		"id bigint,",
		"tenant varchar(50),",
		"org_id bigint,",
		"construct_status varchar(50),",
		"is_removed tinyint(1),",
		"primary key(id, tenant)",
		")",
	}, "\n")
	var createTableSQL2 = strings.Join([]string{
		"create table organization (",
		"id bigint,",
		"tenant varchar(50),",
		"org_type varchar(50),",
		"ext_type varchar(50),",
		"is_removed tinyint(1),",
		"primary key(id, tenant)",
		")",
	}, "\n")
	tk.MustExec(createTableSQL0)
	tk.MustExec(createTableSQL1)
	tk.MustExec(createTableSQL2)

	tk.MustExec(`insert into org_relation
(tenant, org_id, child_org_id)
values
('mctest', 466585195139584, 466585195139584)
,('mctest', 466585369301504, 466585369301504)
,('crec4', 1241041712902656, 1241041712902656)
,('crec4', 1241042011738624, 1241042191774208)
`)
	tk.MustExec(`insert into project
(tenant, id, org_id, construct_status, is_removed)
values
('mctest', 466585195139584, 466585195139584, 'finished', 0)
,('mctest', 466585369301504, 466585369301504, 'none', 0)
,('crec4', 1241041712902656, 1241041712902656, 'none', 0)
,('crec4', 1241042011738624, 1241042011738624, 'none', 0)
`)
	tk.MustExec(`insert into organization
(id, tenant, org_type, ext_type, is_removed)
values
(466585195139584, 'mctest', 'project', 'project', 0)
,(466585369301504, 'mctest', 'project', 'project', 0)
,(1241041712902656, 'crec4', 'project', 'project', 0)
,(1241042011738624, 'crec4', 'project', 'project', 0)
`)

	sql := `WITH orgs AS (
		SELECT tenant,child_org_id FROM global_platform.org_relation WHERE org_id IS NOT NULL AND IFNULL(org_id, '') IN(
			SELECT DISTINCT IFNULL(org_id, '') FROM global_platform.project WHERE org_id IS NOT NULL AND (
is_removed=FALSE AND construct_status IN (?, ?, ?)
			AND org_id IS NOT NULL AND IFNULL(org_id, '') IN(SELECT DISTINCT IFNULL(id, '') FROM global_platform.organization WHERE id IS NOT NULL AND (
org_type = ? AND ext_type = ? AND is_removed = FALSE
))
)
		)
)SELECT * FROM orgs`
	return tk, sql
}

type getServiceCase struct {
	sql     string
	service string
}

func TestGetSeriveFromSql(t *testing.T) {
	cases := []*getServiceCase{
		{"/* from:'tenant-service', host */ select 1", "tenant-service"},
		{"select 1", ""},
	}
	for _, c := range cases {
		service := executor.GetSeriveFromSQL(c.sql)
		require.Equal(t, c.service, service)
	}
}

func TestLargeQueryWithoutLogFile(t *testing.T) {
	store := testkit.CreateMockStore(t)

	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Metrics.LargeQuery.Filename": "mctech-large-query-exist.log"}),
	)
	// cfg := config.GetMCTechConfig()
	tk := testkit.NewTestKit(t, store)
	// tk.MustExec(fmt.Sprintf("set @@mctech_metrics_large_query_file='%v'", cfg.Metrics.LargeQuery.Filename))
	tk.MustQuery("select query from information_schema.mctech_large_query").Check(testkit.Rows())
	tk.MustQuery("select query from information_schema.mctech_large_query where time > '2020-09-15 12:16:39' and time < now()").Check(testkit.Rows())
	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}

func TestLargeQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	f, err := os.CreateTemp("", "mctech-large-query-*.log")
	require.NoError(t, err)

	resultFields := []string{
		"# TIME: 2020-10-13T20:08:13.970563+08:00",
		"# DIGEST: 0368dd12858f813df842c17bcb37ca0e8858b554479bebcd78da1f8c14ad12d0",
		"select * from t0;",
		"# TIME: 2020-10-16T20:08:13.970563+08:00",
		"# DIGEST: 0368dd12858f813df842c17bcb37ca0e8858b554479bebcd78da1f8c14ad12d0",
		"select * from t2;",
		"# TIME: 2022-04-21T14:44:54.103041447+08:00",
		"# USER@HOST: root[root] @ 192.168.0.1 [192.168.0.1]",
		"# QUERY_TIME: 1",
		"# PARSE_TIME: 0.00000001",
		"# COMPILE_TIME: 0.00000001",
		"# REWRITE_TIME: 0.000000003",
		"# OPTIMIZE_TIME: 0.00000001",
		"# PROCESS_TIME: 2 WAIT_TIME: 60 TOTAL_KEYS: 10000",
		"# DB: test",
		"# DIGEST: 01d00e6e93b28184beae487ac05841145d2a2f6a7b16de32a763bed27967e83d",
		"# MEM_MAX: 2333",
		"# DISK_MAX: 6666",
		"# RESULT_ROWS: 12345",
		"# SUCC: true",
		"# SQL_LENGTH: 16",
		"{gzip}H4sIAAAAAAAA/ypOzUlNLlHQUkgrys9VKLEGBAAA///MPyzQEAAAAA==;",
	}

	_, err = f.WriteString(strings.Join(resultFields, "\n"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	batchSize := executor.ParseLargeQueryBatchSize
	executor.ParseLargeQueryBatchSize = 1
	defer func() {
		executor.ParseLargeQueryBatchSize = batchSize
		require.NoError(t, os.Remove(f.Name()))
	}()

	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Metrics.LargeQuery.Filename": f.Name()}),
	)

	tk.MustQuery("select count(*) from `information_schema`.`mctech_large_query` where time > '2020-10-16 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from `information_schema`.`mctech_large_query` where time > '2019-10-13 20:08:13' and time < '2020-10-16 21:08:13'").Check(testkit.Rows("2"))
	// Cover tidb issue 34320
	tk.MustQuery("select count(digest) from `information_schema`.`mctech_large_query` where time > '2019-10-13 20:08:13' and time < now();").Check(testkit.Rows("3"))
	tk.MustQuery("select count(digest) from `information_schema`.`mctech_large_query` where time > '2022-04-29 17:50:00'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from `information_schema`.`mctech_large_query` where time < '2010-01-02 15:04:05'").Check(testkit.Rows("0"))

	tk.MustQuery("select query from `information_schema`.`mctech_large_query`").Check(
		testkit.Rows(
			"select * from t0;",
			"select * from t2;",
			"select * from t;",
		))

	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}
