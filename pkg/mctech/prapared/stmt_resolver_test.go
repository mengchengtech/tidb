package prapared

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
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
	err := session.Auth(&auth.UserIdentity{Username: user, Hostname: "%"}, nil, nil, nil)
	require.NoError(t, err)

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
	expect  map[string]any
	failure string
}

var dbMap = map[string]string{
	"pf": "global_platform",
	"pd": "public_data",
	"ac": "asset_component",
}

func doRunTest(t *testing.T, cases []*mctechTestCase) {
	store := testkit.CreateMockStore(t)
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
	err = resolver.ResolveStmt(stmt, charset, collation)
	require.NoErrorf(t, err, "source %v", sql)
	err = resolver.Validate(session)
	if err != nil {
		return err
	}
	info := resolver.Context().GetInfoForTest()
	require.Equal(t, c.expect, info, fmt.Sprintf("db: %s, raw sql:%s,", c.shortDb, c.sql))
	return nil
}

func TestStmtResolverWithRoot(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*mctechTestCase{
		{"test", "select * from company /*& global:true */", map[string]any{"global": map[string]any{"set": true}, "db": "test"}, ""},
		//
		{"pf", "/*& global:!ys2 */ select * from company", map[string]any{"global": map[string]any{"set": true, "excludes": []string{"ys2"}}, "db": "global_platform"}, ""},
		{"pf", "select * from company /*& global:!ys2,!ys3 */", map[string]any{"global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}}, "db": "global_platform"}, ""},
		// hint 格式不匹配
		{"pf", "/* global:true */ select * from company", nil, "用户root所属的角色无法确定租户信息"},
		{"test", "/* global:true */ select * from company", map[string]any{"db": "test"}, ""},
		// tenant hint
		{"pf", "/*& tenant:gdcd */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd"}, "db": "global_platform"}, ""},
		{"pf", "/*& tenant:gdcd */ /*& global:1 */ select * from company", nil, "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"pf", "/*& tenant:gdcd */ /*& requestId:abc123456 */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"requestId": "abc123456", "tenant": "gdcd"}, "db": "global_platform"}, ""},
		// background
		{"pf", "/*& tenant:ztsj */ /*& background:true */ select * from company", map[string]any{"tenant": "ztsj", "params": map[string]any{"tenant": "ztsj", "background": "true"}, "db": "global_platform"}, ""},
		// dbPrefix
		{"pd", "/*& dbPrefix:mock */ select * from company", map[string]any{"prefix": "mock", "params": map[string]any{"dbPrefix": "mock"}, "db": "public_data"}, ""},
	}

	doRunTest(t, cases)
}
