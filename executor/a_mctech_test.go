package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech"

	// 强制调用preps包里的init方法
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	option := mctech.GetOption()
	forbidden := option.ForbiddenPrepare
	option.ForbiddenPrepare = true
	defer func() {
		option.ForbiddenPrepare = forbidden
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []*mctechStmtCases{
		{`prepare st from "select * from information_schema.statements_summary"`, "", "[mctech] PREPARE not allowed"},
	}

	for _, c := range cases {
		tk.MustContainErrMsg(c.source, c.failure)
	}
}

func TestPrepareByQuery(t *testing.T) {
	option := mctech.GetOption()
	forbidden := option.ForbiddenPrepare
	option.ForbiddenPrepare = false
	defer func() {
		option.ForbiddenPrepare = forbidden
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk, sql := initDbAndData(t, store, option)

	session := tk.Session()
	var ctx context.Context
	ctx, _ = mctech.WithNewContext3(context.Background(), session, true)
	tk.MustExecWithContext(
		ctx,
		fmt.Sprintf(`prepare st from "%s"`, sql),
	)

	tk.MustExec(
		"SET @p1 = 'termination', @p2 = 'finished', @p3 = 'none', @p4 = 'project'",
	)

	ctx, _ = mctech.WithNewContext(session)
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
}

func TestPrepareByCmd(t *testing.T) {
	option := mctech.GetOption()
	forbidden := option.ForbiddenPrepare
	option.ForbiddenPrepare = false
	defer func() {
		option.ForbiddenPrepare = forbidden
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk, sql := initDbAndData(t, store, option)

	session := tk.Session()
	var ctx context.Context
	ctx, _ = mctech.WithNewContext3(context.Background(), session, true)
	result1 := tk.MustQueryWithContext(ctx, sql, "termination", "finished", "none", "project", "project", "mctest")

	rows1 := result1.Rows()
	seqs1 := map[string]any{}
	require.Len(t, rows1, 2)
	for _, row := range rows1 {
		seqs1[row[0].(string)] = true
	}
}

func TestPrepareByCmdNoTenant(t *testing.T) {
	option := mctech.GetOption()
	forbidden := option.ForbiddenPrepare
	option.ForbiddenPrepare = false
	defer func() {
		option.ForbiddenPrepare = forbidden
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("select * from information_schema.statements_summary limit ?", 5,)
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

func initDbAndData(t *testing.T, store kv.Storage, option *mctech.Option) (*testkit.TestKit, string) {
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
