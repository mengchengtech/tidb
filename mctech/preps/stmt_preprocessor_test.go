package preps_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/mctech/preps"
	"github.com/stretchr/testify/require"
)

type mctechStmtResolverTestCase struct {
	shortDb     string         // 当前数据库的短名
	sql         string         // 传入的sql语句
	expectDBs   []string       // 期望检测到使用过的数据库列表（排序后）
	expectValue map[string]any // 期望的解析后的数据结构(一般属性)
	failure     string         // 失败后抛出的异常信息
}

func (m *mctechStmtResolverTestCase) Failure() string {
	return m.failure
}

func (m *mctechStmtResolverTestCase) Source(i int) any {
	return fmt.Sprintf("(%d) %s", i, m.sql)
}

func (m *mctechStmtResolverTestCase) Roles() []string {
	return nil
}

func TestStmtResolverWithRoot(t *testing.T) {
	// {prefix:"", tenant:"", tenantFromRole: true, params:{tenant:"", mpp: "", global:{set:true, excludes: [""]}, db:"", comments:{}}}
	cases := []*mctechStmtResolverTestCase{
		{"pf", "/*& tenant:gdcd */ /*& tenant:'gdcd' */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd1 */ select * from company", nil, nil, "多个 tenant hint包含不同的值: gdcd <=> gdcd1"},
		{"test", "describe company", nil, map[string]any{"db": "test", "tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "comments": map[string]any{}}, ""},
		{"test", "select * from company /*& global:true */", []string{"test"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "db": "test", "comments": map[string]any{}}, ""},
		//
		{"pf", "/*& global:true */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"test", "/*& global:true */ select * from company", []string{"test"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "db": "test", "comments": map[string]any{}}, ""},
		{"pf", "/*& global:!ys2 */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2"}}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& global:-ys2 */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2"}}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& global:+ys2 */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "includes": []string{"ys2"}}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "select * from company /*& global:-ys2,!ys3 */", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "select * from company /*& global:+ys2,+ys3 */", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "includes": []string{"ys2", "ys3"}}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& global:!ys2,-ys3 , +ys22 , +ys33 */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}, "includes": []string{"ys22", "ys33"}}, "db": "global_platform", "comments": map[string]any{}}, ""},
		// hint 格式不匹配
		{"pf", "/*  & global:true */ select * from company", nil, nil, "当前用户无法确定所属租户信息"},
		{"pf", "/* global:true */ select * from company", nil, nil, "当前用户无法确定所属租户信息"},
		{"test", "/* global:true */ select * from company", []string{"test"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "db": "test", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:' */ select * from company", nil, nil, "\"tenant\" hint 值格式不正确 -> '"},
		{"pf", "/*& tenant:'gslq */ select * from company", nil, nil, "\"tenant\" hint 值格式不正确 -> 'gslq"},
		{"pf", "/*& tenant: '  gslq */ select * from company", nil, nil, "\"tenant\" hint 值格式不正确 -> '  gslq"},
		{"pf", "/*& tenant:gslq  ' */ select * from company", nil, nil, "\"tenant\" hint 值格式不正确 -> gslq  '"},
		// tenant hint
		{"pf", "/*& tenant:gdcd */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:gdcd */ /*& global:1 */ select * from company", nil, nil, "存在tenant信息时，global不允许设置为true"},
		{"pf", "/*& tenant: gdcd */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:'gdcd ' */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:'  gdcd' */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:'  gdcd   ' */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:  '  gdcd   ' */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant: */ select * from company", nil, nil, "当前用户无法确定所属租户信息"},
		{"pf", "select * from platform.company", []string{"platform"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		// 空值
		{"test", "/*& custom: */ select * from company", []string{"test"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"custom": "", "mpp": "allow"}, "db": "test", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant:'' */ select * from company", nil, nil, "当前用户无法确定所属租户信息"},
		{"pf", "/*& tenant:'    ' */ select * from company", nil, nil, "当前用户无法确定所属租户信息"},

		// request_id
		{"pf", "/*& tenant:gdcd */ /*& requestId:abc123456 */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"requestId": "abc123456", "tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		// background
		{"pf", "/*& tenant:ztsj */ /*& background:true */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "ztsj", "fromRole": false}, "params": map[string]any{"tenant": "ztsj", "background": "true", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		// across
		{"pf", "/*& tenant:ztsj */ /*& across:global_cq3,global_ds */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "ztsj", "fromRole": false}, "params": map[string]any{"tenant": "ztsj", "across": "global_cq3|global_ds", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		// dbPrefix
		{"pd", "/*& dbPrefix:mock */ select * from company", []string{"mock_public_data"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "prefix": "mock", "params": map[string]any{"dbPrefix": "mock", "mpp": "allow"}, "db": "public_data", "comments": map[string]any{}}, ""},
		// replace
		{"pd", "/*& $replace:tenant */ /*& tenant:gslq */ select * from company", []string{"public_data"}, map[string]any{"tenant": map[string]any{"code": "gslq", "fromRole": false}, "params": map[string]any{"tenant": "gslq", "mpp": "allow"}, "db": "public_data", "comments": map[string]any{}}, ""},   // replace
		{"pd", "/*& $replace:tenant */ /*& tenant:'gslq' */ select * from company", []string{"public_data"}, map[string]any{"tenant": map[string]any{"code": "gslq", "fromRole": false}, "params": map[string]any{"tenant": "gslq", "mpp": "allow"}, "db": "public_data", "comments": map[string]any{}}, ""}, // replace
		{"pd", "/*& $replace:tenant=mctech */ select * from company", []string{"public_data"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "db": "public_data", "comments": map[string]any{}}, ""},
		{"pd", "/*& $replace:tenant */ select * from company", nil, nil, "执行[replace]时未找到名称为'tenant'的参数的值"},

		// 新的值声明方式
		{"pf", "/*& tenant|gdcd */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
		{"pf", "/*& tenant|gdcd */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},

		// 租户隔离角色
		{"pf", "/*& impersonate: tenant */ select * from company", nil, nil, "impersonate的值错误。可选值为'tenant_only'"},
		{"pf", "/*& impersonate: tenant_only */ select * from company", nil, nil, "当前用户无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */"},
		{"pf", "/*& global:true */ /*& impersonate: tenant_only */ select * from company", nil, nil, "当前用户包含'租户隔离'角色，不允许启用 'global' hint"},
		{"pf", "/*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "impersonate": "tenant_only", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},

		// custom comment
		{"pf", "/* from:'demo-service' */ /*& tenant:gdcd */ /*& tenant:'gdcd' */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"service": "demo-service"}}, ""},
		{"pf", "/* from:'another-demo-service' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"impersonate": "tenant_only", "mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"service": "another-demo-service"}}, ""},
		{"pf", "/* from:'demo-service.pf' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"impersonate": "tenant_only", "mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"service": "demo-service.pf"}}, ""},
		{"pf", "/* from:'demo-service.pc' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"impersonate": "tenant_only", "mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"service": "demo-service.pc"}}, ""},
		{"pf", "/* from:'another-demo-service.pf' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"impersonate": "tenant_only", "mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"service": "another-demo-service.pf"}}, ""},
		{"pf", "/* package:'@mctech/dp-impala' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"impersonate": "tenant_only", "mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"pkg": "@mctech/dp-impala"}}, ""},
		{"pf", "/* package:'@mctech/another-dp-impala' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"impersonate": "tenant_only", "mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"pkg": "@mctech/another-dp-impala"}}, ""},
		{"pf", "/* from:'demo-service' */ /* package:'@mctech/dp-impala' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", []string{"global_platform"}, map[string]any{"db": "global_platform", "tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"impersonate": "tenant_only", "mpp": "allow", "tenant": "gdcd"}, "comments": map[string]any{"service": "demo-service", "pkg": "@mctech/dp-impala"}}, ""},
	}

	doRunWithSessionTest(t, stmtResoverRunTestCase, cases)
}

func TestStmtResolverNormalizeDB(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/preps/SetSQLDBS",
		mock.M(t, "global_mtlp,global_qa,global_platform,global_cq3,global_mtlp,global_cq3"))
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/preps/SetSQLDBS")

	// {prefix:"", tenant:"", tenantFromRole: true, params:{tenant:"", mpp: "", global:{set:true, excludes: [""]}, db:"", comments:{}}}
	cases := []*mctechStmtResolverTestCase{
		{"pf", "/*& tenant|gdcd */ select * from company", []string{"global_cq3", "global_mtlp", "global_platform", "global_qa"}, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "db": "global_platform", "comments": map[string]any{}}, ""},
	}
	doRunWithSessionTest(t, stmtResoverRunTestCase, cases)
}

func stmtResoverRunTestCase(t *testing.T, i int, c *mctechStmtResolverTestCase, mctechCtx mctech.Context) error {
	db, ok := dbMap[c.shortDb]
	if !ok {
		db = "test"
	}

	var (
		sql     = c.sql
		session = mctechCtx.Session()
		result  mctech.PrepareResult
		err     error
	)

	session.GetSessionVars().CurrentDB = db
	sql, result, err = preps.GetPreprocessorForTest().PrepareSQL(mctechCtx, sql)
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

	dbs, skipped, err := preps.GetPreprocessorForTest().ResolveStmt(mctechCtx, stmt, charset, collation)
	if err != nil {
		return err
	}

	if err = preps.GetDatabaseCheckerForTest().Check(mctechCtx, stmt, dbs); err != nil {
		return err
	}
	if !skipped {
		err = preps.GetPreprocessorForTest().Validate(mctechCtx)
		if err != nil {
			return err
		}
	}
	info := mctechCtx.(mctech.ContextForTest).GetInfoForTest()
	if c.expectDBs == nil {
		c.expectDBs = []string{}
	}
	if c.expectValue == nil {
		c.expectValue = map[string]any{}
	}
	require.Equal(t, c.expectDBs, dbs, c.Source(i))
	require.Equal(t, c.expectValue, info, c.Source(i))
	return nil
}
