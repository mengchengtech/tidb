package prapared

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/session/types"
	"github.com/stretchr/testify/require"
)

type mctechStmtResolverTestCase struct {
	shortDb string
	sql     string
	expect  map[string]any
	failure string
}

func (m *mctechStmtResolverTestCase) Failure() string {
	return m.failure
}

func (m *mctechStmtResolverTestCase) Source() any {
	return m.sql
}

func TestStmtResolverWithRoot(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*mctechStmtResolverTestCase{
		{"test", "describe company", map[string]any{"db": "test"}, ""},
		{"test", "select * from company /*& global:true */", map[string]any{"global": map[string]any{"set": true}, "db": "test"}, ""},
		//
		{"pf", "/*& global:true */ select * from company", map[string]any{"global": map[string]any{"set": true}, "db": "global_platform"}, ""},
		{"test", "/*& global:true */ select * from company", map[string]any{"global": map[string]any{"set": true}, "db": "test"}, ""},
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
		// replace
		{"pd", "/*& $replace:tenant */ /*& tenant:gslq */ select * from company", map[string]any{"tenant": "gslq", "params": map[string]any{"tenant": "gslq"}, "db": "public_data"}, ""},   // replace
		{"pd", "/*& $replace:tenant */ /*& tenant:'gslq' */ select * from company", map[string]any{"tenant": "gslq", "params": map[string]any{"tenant": "gslq"}, "db": "public_data"}, ""}, // replace
		{"pd", "/*& $replace:tenant=mctech */ select * from company", map[string]any{"db": "public_data"}, ""},
		{"pd", "/*& $replace:tenant */ select * from company", nil, "执行[replace]时未找到名称为'tenant'的参数的值"},
	}

	doRunWithSessionTest(t, stmtResoverRunTestCase, cases, "root")
}

func stmtResoverRunTestCase(t *testing.T, c *mctechStmtResolverTestCase, session types.Session) error {
	resolver := &mctechStatementResolver{
		checker: newMutexDatabaseChecker(),
	}
	db, ok := dbMap[c.shortDb]
	if !ok {
		db = "test"
	}

	sql := c.sql
	session.GetSessionVars().CurrentDB = db
	sql, err := resolver.PrepareSQL(session, sql)
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
	dbs, skipped, err := resolver.ResolveStmt(stmt, charset, collation)
	if err != nil {
		return err
	}

	if err = resolver.CheckDB(dbs); err != nil {
		return err
	}
	if !skipped {
		err = resolver.Validate(session)
		if err != nil {
			return err
		}
	}
	info := resolver.Context().GetInfoForTest()
	require.Equal(t, c.expect, info, c.Source())
	return nil
}