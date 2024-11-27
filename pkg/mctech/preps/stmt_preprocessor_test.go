package preps

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/mctech"
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
		{"pf", "/*& tenant:gdcd */ /*& tenant:'gdcd' */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform"}, ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform"}, ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd1 */ select * from company", nil, "多个 tenant hint包含不同的值: gdcd <=> gdcd1"},
		{"test", "describe company", map[string]any{"db": "test", "params": map[string]any{"mpp": "allow"}}, ""},
		{"test", "select * from company /*& global:true */", map[string]any{"params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "db": "test"}, ""},
		//
		{"pf", "/*& global:true */ select * from company", map[string]any{"params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "db": "global_platform"}, ""},
		{"test", "/*& global:true */ select * from company", map[string]any{"params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "db": "test"}, ""},
		{"pf", "/*& global:!ys2 */ select * from company", map[string]any{"params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2"}}, "db": "global_platform"}, ""},
		{"pf", "select * from company /*& global:!ys2,!ys3 */", map[string]any{"params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}}, "db": "global_platform"}, ""},
		// hint 格式不匹配
		{"pf", "/*  & global:true */ select * from company", nil, "当前用户root无法确定所属租户信息"},
		{"pf", "/* global:true */ select * from company", nil, "当前用户root无法确定所属租户信息"},
		{"test", "/* global:true */ select * from company", map[string]any{"params": map[string]any{"mpp": "allow"}, "db": "test"}, ""},
		// tenant hint
		{"pf", "/*& tenant:gdcd */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform"}, ""},
		{"pf", "/*& tenant:gdcd */ /*& global:1 */ select * from company", nil, "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"pf", "/*& tenant:gdcd */ /*& requestId:abc123456 */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"requestId": "abc123456", "tenant": "gdcd", "mpp": "allow"}, "db": "global_platform"}, ""},
		// background
		{"pf", "/*& tenant:ztsj */ /*& background:true */ select * from company", map[string]any{"tenant": "ztsj", "params": map[string]any{"tenant": "ztsj", "background": "true", "mpp": "allow"}, "db": "global_platform"}, ""},
		// dbPrefix
		{"pd", "/*& dbPrefix:mock */ select * from company", map[string]any{"prefix": "mock", "params": map[string]any{"dbPrefix": "mock", "mpp": "allow"}, "db": "public_data"}, ""},
		// replace
		{"pd", "/*& $replace:tenant */ /*& tenant:gslq */ select * from company", map[string]any{"tenant": "gslq", "params": map[string]any{"tenant": "gslq", "mpp": "allow"}, "db": "public_data"}, ""},   // replace
		{"pd", "/*& $replace:tenant */ /*& tenant:'gslq' */ select * from company", map[string]any{"tenant": "gslq", "params": map[string]any{"tenant": "gslq", "mpp": "allow"}, "db": "public_data"}, ""}, // replace
		{"pd", "/*& $replace:tenant=mctech */ select * from company", map[string]any{"params": map[string]any{"mpp": "allow"}, "db": "public_data"}, ""},
		{"pd", "/*& $replace:tenant */ select * from company", nil, "执行[replace]时未找到名称为'tenant'的参数的值"},

		// 新的值声明方式
		{"pf", "/*& tenant|gdcd */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform"}, ""},
		{"pf", "/*& tenant|'gdcd' */ select * from company", map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform"}, ""},
	}

	doRunWithSessionTest(t, stmtResoverRunTestCase, cases, "root")
}

func stmtResoverRunTestCase(t *testing.T, c *mctechStmtResolverTestCase, mctechCtx mctech.Context) error {
	db, ok := dbMap[c.shortDb]
	if !ok {
		db = "test"
	}

	var (
		sql     = c.sql
		session = mctechCtx.Session()
		result  *mctech.PrepareResult
		err     error
	)

	session.GetSessionVars().CurrentDB = db
	sql, result, err = preprocessor.PrepareSQL(mctechCtx, sql)
	if err != nil {
		return err
	}
	modifyCtx := mctechCtx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetPrepareResult(result)
	ctx := context.Background()
	stmts, err := session.(parser).Parse(ctx, sql)
	if err != nil {
		return err
	}
	stmt := stmts[0]
	charset, collation := session.GetSessionVars().GetCharsetInfo()
	modifyCtx.Reset()

	dbs, skipped, err := preprocessor.ResolveStmt(mctechCtx, stmt, charset, collation)
	if err != nil {
		return err
	}

	if err = getDatabaseChecker().Check(mctechCtx, stmt, dbs); err != nil {
		return err
	}
	if !skipped {
		err = preprocessor.Validate(mctechCtx)
		if err != nil {
			return err
		}
	}
	info := mctechCtx.(mctech.ContextForTest).GetInfoForTest()
	require.Equal(t, c.expect, info, c.Source())
	return nil
}
