package executor_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
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

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_platform")
	tk.MustExec("create database global_platform")
	tk.MustExec("use global_platform")
	s := tk.Session()
	s.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}
	return tk
}

func TestPrepare(t *testing.T) {
	option := mctech.GetOption()
	forbidden := option.ForbiddenPrepare
	option.ForbiddenPrepare = false
	defer func() {
		option.ForbiddenPrepare = forbidden
	}()

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)

	var createTableSQL = strings.Join([]string{
		"create table demo (",
		"id bigint,",
		"tenant varchar(50),",
		"b int,",
		"primary key(id, tenant)",
		")",
	}, "\n")
	tk.MustExec(createTableSQL)
	tk.MustExec("insert into demo (id, tenant, b) values (1, 'gslq', 1), (2, 'gslq', 2)")
	tk.MustExec("insert into demo (id, tenant, b) values (5, 'ztsj', 5), (6, 'ztsj', 6)")

	session := tk.Session()
	mctechCtx := mctech.NewContext(session, false)
	mctech.SetContextForTest(session, mctechCtx)
	tk.MustExec(`prepare st from "select * from demo a
	join demo b on a.id = b.id
	where a.id < ?"`)

	mctechCtx = mctech.NewContext(session, false)
	mctech.SetContextForTest(session, mctechCtx)
	tk.MustExec("set @p1 = 6")

	mctechCtx = mctech.NewContext(session, false)
	mctech.SetContextForTest(session, mctechCtx)
	result := tk.MustQuery(`/*& tenant:gslq */ execute st using @p1`)
	print(result)
}
