// add by zhangbing

package executor_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	mcworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
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
		{
			"mctech select * from information_schema.statements_summary",
			strings.Join(
				[]string{
					"<nil>", // global
					"<nil>", // excludes
					"<nil>", // includes
					"<nil>", // comments
					"<nil>", // tenant
					"<nil>", // tenant_from
					"test",  // db
					"<nil>", // dbs
					"<nil>", // tables,
					"<nil>", // dw_index
					"<nil>", // params
					"SELECT * FROM `information_schema`.`statements_summary`", // prepared_sql
				}, "|"),
			"",
		},
	}

	for _, c := range cases {
		tk.MustQuery(c.source).Check(
			testkit.RowsWithSep("|", c.expect))
	}
}

func TestForbiddenPrepare(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": true}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []*mctechStmtCases{
		{`prepare st from "select * from information_schema.statements_summary"`, "", "[mctech] PREPARE not allowed"},
	}

	for _, c := range cases {
		tk.MustContainErrMsg(c.source, c.failure)
	}
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

func TestPrepareAndExecuteByQuery(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": false, "Tenant.Enabled": true}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/EnsureContext", mock.M(t, "true"))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/EnsureContext")
	}()

	store := testkit.CreateMockStore(t)
	tk, sql := initDbAndData(t, store)

	tk.MustExecWithContext(
		context.Background(),
		fmt.Sprintf(`prepare st from "%s"`, sql),
	)

	tk.MustExec(
		"SET @p1 = 'termination', @p2 = 'finished', @p3 = 'none', @p4 = 'project'",
	)

	rs := tk.MustQueryWithContext(
		context.Background(),
		`/*& tenant:mctest */ EXECUTE st USING @p1, @p2, @p3, @p4, @p4`,
	)

	rows1 := rs.Rows()
	seqs1 := map[string]any{}
	require.Len(t, rows1, 2)
	for _, row := range rows1 {
		seqs1[row[0].(string)] = true
	}
}

func TestPrepareAndExecuteByCmd(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": false, "Tenant.Enabled": true}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/EnsureContext", mock.M(t, "true"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/EnsureContext")

	store := testkit.CreateMockStore(t)
	tk, sql := initDbAndData(t, store)

	result := tk.MustQueryWithContext(context.Background(), sql, "termination", "finished", "none", "project", "project", "mctest")

	rows := result.Rows()
	seqs := map[string]any{}
	require.Len(t, rows, 2)
	for _, row := range rows {
		seqs[row[0].(string)] = true
	}
}

func TestPrepareAndExecuteByCmdDispatch(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()

	store := testkit.CreateMockStore(t)
	tk, sql := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	ctx := context.Background()
	data := append([]byte{mysql.ComStmtPrepare}, []byte(sql)...)
	require.NoError(t, cc.Dispatch(ctx, data))
	data = []byte{
		mysql.ComStmtExecute, // cmd
		0x01, 0x0, 0x0, 0x0,  // stmtID little endian
		0x00,               // useCursor == false
		0x1, 0x0, 0x0, 0x0, // iteration-count, always 1
		0x0,      // 表示空值的位信息。一个字节可以表示8个参数的可空信息
		0x1,      // new-params-bound-flag
		0xfe, 00, // string type
		0xfe, 00, // string type
		0xfe, 00, // string type
		0xfe, 00, // string type
		0xfe, 00, // string type
		0xfe, 00, // string type
		0xfc, 0xb, 0x0, 't', 'e', 'r', 'm', 'i', 'n', 'a', 't', 'i', 'o', 'n', // string value 'termination'
		0xfc, 0x8, 0x0, 'f', 'i', 'n', 'i', 's', 'h', 'e', 'd', // string value 'finished'
		0xfc, 0x4, 0x0, 'n', 'o', 'n', 'e', // string value 'none'
		0xfc, 0x7, 0x0, 'p', 'r', 'o', 'j', 'e', 'c', 't', // string value 'project'
		0xfc, 0x7, 0x0, 'p', 'r', 'o', 'j', 'e', 'c', 't', // string value 'project'
		0xfc, 0x6, 0x0, 'm', 'c', 't', 'e', 's', 't', // string value 'mctest'
	}

	require.NoError(t, cc.Dispatch(ctx, data))
}

func TestPrepareAndExecuteByQueryDispatch(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()

	store := testkit.CreateMockStore(t)
	tk, sql := initDbAndData(t, store)
	tk.MustExecWithContext(
		context.Background(),
		fmt.Sprintf(`prepare st from "%s"`, sql),
	)
	tk.MustExec("set @p0='termination',@p1='finished',@p2='none',@p3='project',@p4='project',@p5='mctest'")
	tk.MustQueryWithContext(context.Background(), "execute st using @p0,@p1,@p2,@p3,@p4,@p5")
}

func TestPrepareAndExecuteByQueryNotPassTenantCode(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()

	store := testkit.CreateMockStore(t)
	tk, sql := initDbAndData(t, store)
	tk.MustExecWithContext(
		context.Background(),
		fmt.Sprintf(`prepare st from "%s"`, sql),
	)
	tk.MustExec("set @p0='termination',@p1='finished',@p2='none',@p3='project',@p4='project'")
	_, err := tk.ExecWithContext(context.Background(), "execute st using @p0,@p1,@p2,@p3,@p4")
	require.Error(t, err, "当前用户无法确定所属租户信息，请确认在参数列表最后额外添加了一个非空的租户code参数")
}

func TestPrepareAndExecuteByCmdNoTenant(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.ForbiddenPrepare": false}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/EnsureContext", mock.M(t, "true"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/EnsureContext")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("select * from information_schema.statements_summary limit ?", 5)
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

	tk.MustExec(`/*& global:true */
insert into org_relation
(tenant, org_id, child_org_id)
values
('mctest', 466585195139584, 466585195139584)
,('mctest', 466585369301504, 466585369301504)
,('crec4', 1241041712902656, 1241041712902656)
,('crec4', 1241042011738624, 1241042191774208)
`)
	tk.MustExec(`/*& global:true */
insert into project
(tenant, id, org_id, construct_status, is_removed)
values
('mctest', 466585195139584, 466585195139584, 'finished', 0)
,('mctest', 466585369301504, 466585369301504, 'none', 0)
,('crec4', 1241041712902656, 1241041712902656, 'none', 0)
,('crec4', 1241042011738624, 1241042011738624, 'none', 0)
`)
	tk.MustExec(`/*& global:true */
insert into organization
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

func TestTableTTLInfoNormalColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec(`create table test.ttl_normal_column_demo (
		id bigint,
		created_at datetime
	) ttl created_at + interval 1 year ttl_enable = 'on' ttl_job_interval '3h'
	`)
	tk.MustQuery(
		`select TABLE_SCHEMA, TABLE_NAME, TTL_COLUMN_NAME, TTL_COLUMN_TYPE, TTL_COLUMN_GENERATED_EXPR, TTL, TTL_UNIT, TTL_ENABLE, TTL_JOB_INTERVAL
	from information_schema.mctech_table_ttl_info`,
	).Check(testkit.Rows("test ttl_normal_column_demo created_at datetime  1 YEAR ON 3h"))
}

func TestTableTTLInfoGenerateDateLiteralColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec(`create table test.ttl_generated_column_demo (
		id bigint,
		is_removed boolean,
		updated_at datetime,
		__discarded_at datetime AS (IF (is_removed, updated_at, '9999-12-31 23:59:59'))
	) ttl __discarded_at + interval 180 day ttl_enable = 'on' ttl_job_interval '3h'
	`)
	tk.MustQuery(
		`select TABLE_SCHEMA, TABLE_NAME, TTL_COLUMN_NAME, TTL_COLUMN_TYPE, TTL_COLUMN_GENERATED_EXPR, TTL, TTL_UNIT, TTL_ENABLE, TTL_JOB_INTERVAL
	from information_schema.mctech_table_ttl_info`,
	).Check(testkit.Rows(
		"test ttl_generated_column_demo __discarded_at datetime if(`is_removed`, `updated_at`, _utf8mb4'9999-12-31 23:59:59') 180 DAY ON 3h",
	))
}

func TestTableTTLInfoGeneratedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec(`create table test.ttl_generated_column_demo (
		id bigint,
		is_removed boolean,
		updated_at datetime,
		__discarded_at datetime AS (IF(is_removed, updated_at, date_add(updated_at, INTERVAL 180 DAY)))
	) ttl __discarded_at + interval 180 day ttl_enable = 'on' ttl_job_interval '3h'
	`)
	tk.MustQuery(
		`select TABLE_SCHEMA, TABLE_NAME, TTL_COLUMN_NAME, TTL_COLUMN_TYPE, TTL_COLUMN_GENERATED_EXPR, TTL, TTL_UNIT, TTL_ENABLE, TTL_JOB_INTERVAL
	from information_schema.mctech_table_ttl_info`,
	).Check(testkit.Rows(
		"test ttl_generated_column_demo __discarded_at datetime if(`is_removed`, `updated_at`, date_add(`updated_at`, interval 180 day)) 180 DAY ON 3h",
	))
}

func TestLargeQueryWithoutLogFile(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Metrics.LargeQuery.Filename": "mctech-large-query-exist.log"}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	store := testkit.CreateMockStore(t)
	// cfg := config.GetMCTechConfig()
	tk := testkit.NewTestKit(t, store)
	// tk.MustExec(fmt.Sprintf("set @@mctech_metrics_large_query_file='%v'", cfg.Metrics.LargeQuery.Filename))
	tk.MustQuery("select query from information_schema.mctech_large_query").Check(testkit.Rows())
	tk.MustQuery("select query from information_schema.mctech_large_query where time > '2020-09-15 12:16:39' and time < now()").Check(testkit.Rows())
}

func TestLargeQuery(t *testing.T) {
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
		"# DIGEST: e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7",
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

	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Metrics.LargeQuery.Filename": f.Name()}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

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
}

func TestQueryMCTechCrossDBLoadInfo(t *testing.T) {
	at := time.Now()
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/inject-loaded-at", fmt.Sprintf("return(%d)", at.UnixMilli()))
	failpoint.Enable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade", mock.M(t, "false"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/inject-loaded-at")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade")

	session.RegisterMCTechUpgradeForTest("cross-db", initMCTechCrossDB)
	defer session.UnregisterMCTechUpgradeForTest("cross-db")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	atValue := at.Format("2006-01-02 15:04:05.000")
	rows := [][]any{
		{"1", "*", "both", "0", "global_mtlp,global_ma", "1", "同一条sql语句中允许同时使用给定的数据库", atValue, "success", "Loaded Success", "0", "*", "*", "[global_mtlp global_ma]", "<nil>", "<nil>"},
		{"2", "*", "both", "0", "global_platform,global_ipm,*", "1", "规则里其中一项为'*'时，其它数据库排除在任意规则检查之外", atValue, "success", "Loaded Success", "0", "*", "*", "[]", "1", "[global_ipm global_platform]"},
		{"3", "*", "both", "0", "global_dw_*,global_dwb,*", "1", "规则里其中一项为'*'时，其它数据库排除在任意规则检查之外", atValue, "success", "Loaded Success", "0", "*", "*", "[]", "1", "[global_dw_* global_dwb]"},
		{"4", "@mctech/dp-impala-tidb-enhanced", "package", "1", "", "1", "删除约束检查里跨库约束规则检查，需要允许任意配置的跨库规则", atValue, "success", "Loaded Success", "1", "", "@mctech/dp-impala-tidb-enhanced", "[]", "<nil>", "<nil>"},
		{"1001", "invoker-1", "service", "0", "global_cq3,global_ec5", "1", "", atValue, "success", "Loaded Success", "0", "invoker-1", "", "[global_cq3 global_ec5]", "<nil>", "<nil>"},
		{"1002", "invoker-2", "service", "0", "global_cq2,global_ec5,global_mp", "1", "", atValue, "success", "Loaded Success", "0", "invoker-2", "", "[global_cq2 global_ec5 global_mp]", "<nil>", "<nil>"},
		{"1003", "invoker-2", "service", "0", "global_qa,global_ec3", "1", "", atValue, "success", "Loaded Success", "0", "invoker-2", "", "[global_qa global_ec3]", "<nil>", "<nil>"},
		{"1004", "invoker-empty", "package", "0", "", "1", "", atValue, "error", "Ignore. The 'cross_dbs' field is empty.", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"},
		{"1005", "invoker-one-db", "package", "0", "global_qa", "1", "", atValue, "error", "Ignore. The number of databases in group(0) is less than 2.", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"},
		{"1006", "*", "both", "0", "global_qa,global_mp", "1", "", atValue, "success", "Loaded Success", "0", "*", "*", "[global_qa global_mp]", "<nil>", "<nil>"},
		{"1007", "*", "both", "1", "", "1", "", atValue, "error", "Ignore. The 'allow_all_dbs' field should not be false, when invoker_name is '*'.", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"},
		{"1008", "", "both", "1", "", "1", "", atValue, "error", "Ignore. The 'invoker_name' field is empty.", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"},
		{"1009", "", "both", "0", "", "1", "", atValue, "error", "Ignore. The 'invoker_name' field is empty.", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"},
		{"1010", "invoker-exclude", "service", "0", "global_ds,global_bc,*", "1", "", atValue, "success", "Loaded Success", "0", "invoker-exclude", "", "[]", "0", "[global_bc global_ds]"},
		{"1050", "invoker-allow-all", "both", "1", "", "1", "", atValue, "success", "Loaded Success", "1", "invoker-allow-all", "invoker-allow-all", "[]", "<nil>", "<nil>"},
		{"1100", "invoker-disable", "package", "0", "global_qa, global_sq", "0", "", atValue, "disabled", "current rule is Disabled", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"},
		{"1101", "", "service", "1", "", "0", "", atValue, "disabled", "current rule is Disabled", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>", "<nil>"},
	}

	tk.MustQuery("select * from information_schema.mctech_cross_db_load_info").Check(rows)
}

func initMCTechCrossDB(ctx context.Context, sctx sessiontypes.Session) error {
	ctx = util.WithInternalSourceType(ctx, "initMCTechCrossDB")
	args := []any{
		mysql.SystemDB, mcworker.MCTechCrossDB,
	}
	_, err := sctx.ExecuteInternal(ctx, `insert into %n.%n
	(id, invoker_name, invoker_type, allow_all_dbs, cross_dbs, enabled, created_at)
	values
	(1001, 'invoker-1', 'service', false, 'global_cq3,global_ec5', true, '2024-05-01')
	, (1002, 'invoker-2', 'service', false, 'global_cq2,global_ec5,global_mp',true, '2024-05-01')
	, (1003, 'invoker-2', 'service', false, 'global_qa,global_ec3', true, '2024-05-01')
	, (1004, 'invoker-empty', 'package', false, '', true, '2024-05-01')
	, (1005, 'invoker-one-db', 'package', false, 'global_qa', true, '2024-05-01')
	, (1006, '*', 'both', false, 'global_qa,global_mp', true, '2024-05-01')
	, (1007, '*', 'both', true, '', true, '2024-05-01')
	, (1008, '', 'both', true, '', true, '2024-05-01')
	, (1009, '', 'both', false, '', true, '2024-05-01')
	, (1010, 'invoker-exclude', 'service', false, 'global_ds,global_bc,*', true, '2024-05-01')
	, (1050, 'invoker-allow-all', 'both', true, '', true, '2024-05-01')
	, (1100, 'invoker-disable', 'package', false, 'global_qa, global_sq', false, '2024-05-01')
	, (1101, '', 'service', true, '', false, '2024-05-01')
	`,
		args...)
	return err
}
