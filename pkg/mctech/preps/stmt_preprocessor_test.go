package preps

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/session"
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
		{"pf", "/*& tenant:gdcd */ /*& tenant:'gdcd' */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd"}, "db": "global_platform"}, ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd"}, "db": "global_platform"}, ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd1 */ select * from company", nil, "多个 tenant hint包含不同的值: gdcd <=> gdcd1"},
		{"test", "describe company", map[string]any{"db": "test"}, ""},
		{"test", "select * from company /*& global:true */", map[string]any{"global": map[string]any{"set": true}, "db": "test"}, ""},
		//
		{"pf", "/*& global:true */ select * from company", map[string]any{"global": map[string]any{"set": true}, "db": "global_platform"}, ""},
		{"test", "/*& global:true */ select * from company", map[string]any{"global": map[string]any{"set": true}, "db": "test"}, ""},
		{"pf", "/*& global:!ys2 */ select * from company", map[string]any{"global": map[string]any{"set": true, "excludes": []string{"ys2"}}, "db": "global_platform"}, ""},
		{"pf", "select * from company /*& global:!ys2,!ys3 */", map[string]any{"global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}}, "db": "global_platform"}, ""},
		// hint 格式不匹配
		{"pf", "/* global:true */ select * from company", nil, "当前用户root无法确定所属租户信息"},
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

func stmtResoverRunTestCase(t *testing.T, c *mctechStmtResolverTestCase, session session.Session) error {
	preprocessor := &mctechStatementPreprocessor{
		checker: getMutexDatabaseChecker(),
	}
	db, ok := dbMap[c.shortDb]
	if !ok {
		db = "test"
	}

	sql := c.sql
	session.GetSessionVars().CurrentDB = db
	sql, err := preprocessor.PrepareSQL(session, sql)
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
	preprocessor.Context().Reset()
	dbs, skipped, err := preprocessor.ResolveStmt(stmt, charset, collation)
	if err != nil {
		return err
	}

	if err = preprocessor.CheckDB(dbs); err != nil {
		return err
	}
	if !skipped {
		err = preprocessor.Validate(session)
		if err != nil {
			return err
		}
	}
	info := preprocessor.Context().GetInfoForTest()
	require.Equal(t, c.expect, info, c.Source())
	return nil
}
