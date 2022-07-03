package prapared

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/auth"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_platform")
	tk.MustExec("create database global_platform")
	tk.MustExec("use global_platform")
	tk.MustExec("create table t(a int, b int, key(b))")

	return tk
}

func createSession(t *testing.T, tk *testkit.TestKit, user string, roles []string) session.Session {
	session := tk.Session()
	if user == "" {
		user = "root"
	}
	ok := session.Auth(&auth.UserIdentity{Username: user, Hostname: "%"}, nil, nil)
	require.True(t, ok)

	if len(roles) > 0 {
		ar := make([]*auth.RoleIdentity, len(roles))
		for _, r := range roles {
			ar = append(ar, &auth.RoleIdentity{Username: r, Hostname: "%"})
		}
		session.GetSessionVars().ActiveRoles = ar
	}
	return session
}

type mctechTestCase struct {
	shortDb string
	sql     string
	expect  string
	failure string
}

var dbMap = map[string]string{
	"pf": "global_platform",
	"pd": "public_data",
	"ac": "asset_component",
}

func doRunTest(t *testing.T, cases []*mctechTestCase) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)
	session := createSession(t, tk, "root", nil)

	for _, c := range cases {
		err := runTestCase(t, c, session)
		if err == nil {
			continue
		}

		if c.failure != "" {
			require.ErrorContains(t, err, c.failure)
		} else {
			require.NoErrorf(t, err, "source %v", c.sql)
		}
	}
}

func runTestCase(t *testing.T, c *mctechTestCase, session session.Session) error {
	resolver := NewStatementResolver()
	sql := c.sql
	db, ok := dbMap[c.shortDb]
	if !ok {
		db = "test"
	}
	session.GetSessionVars().CurrentDB = db
	sql, err := resolver.PrepareSql(session, sql)
	if err != nil {
		return err
	}
	ctx := context.Background()
	stmts, err := session.Parse(ctx, sql)
	if err != nil {
		return err
	}
	stmt := stmts[0]
	charset, collation := session.GetSessionVars().GetCharsetInfo()
	resolver.Context().Reset()
	if err != nil {
		return err
	}
	err = resolver.ResolveStmt(stmt, charset, collation)
	if err != nil {
		return err
	}
	err = resolver.Validate(session)
	if err != nil {
		return err
	}
	info := resolver.Context().GetInfo()
	require.Equal(t, c.expect, info)
	return nil
}

func TestStmtResolverWithRoot(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*mctechTestCase{
		{"test", "select * from company /*& global:true */", "{{{,,false,[],{true,[]}}},test}", ""},
		//
		{"pf", "/*& global:!ys2 */ select * from company", "{{{,,false,[],{true,[ys2]}}},global_platform}", ""},
		{"pf", "select * from company /*& global:!ys2,!ys3 */", "{{{,,false,[],{true,[ys2 ys3]}}},global_platform}", ""},
		// hint 格式不匹配
		{"pf", "/* global:true */ select * from company", "", "用户root所属的角色无法确定租户信息"},
		{"test", "/* global:true */ select * from company", "{{{,,false,[],{false,[]}}},test}", ""},
		// tenant hint
		{"pf", "/*& tenant:gdcd */ select * from company", "{{{,gdcd,false,[{tenant,gdcd}],{false,[]}}},global_platform}", ""},
		{"pf", "/*& tenant:gdcd */ /*& global:1 */ select * from company", "", "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"pf", "/*& tenant:gdcd */ /*& requestId:abc123456 */ select * from company", "{{{,gdcd,false,[{requestId,abc123456} {tenant,gdcd}],{false,[]}}},global_platform}", ""},
		// background
		{"pf", "/*& tenant:ztsj */ /*& background:true */ select * from company", "{{{,ztsj,false,[{background,true} {tenant,ztsj}],{false,[]}}},global_platform}", ""},
		// dbPrefix
		{"pd", "/*& dbPrefix:mock */ select * from company", "{{{mock,,false,[{dbPrefix,mock}],{false,[]}}},public_data}", ""},
	}

	doRunTest(t, cases)
}

func TestStmtResolverDW_WithRoot(t *testing.T) {
	cases := []*mctechTestCase{
		// dw
		{"pd", "/*& global:true */ select * from global_dw.company", "", "get dw index errors"},
	}
	doRunTest(t, cases)

	option := mctech.GetOption()
	option.DbChecker_ApiPrefix = "http://10.12.6.5:31051/"
	cases = []*mctechTestCase{
		// dw
		{"pd", "/*& global:true */ select * from global_dw.company", "{{{,,false,[],{true,[]}}},public_data}", "get dw index errors"},
	}
	doRunTest(t, cases)
}
