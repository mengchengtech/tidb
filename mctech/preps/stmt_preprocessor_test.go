package preps_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/mctech/preps"
	"github.com/stretchr/testify/require"
)

type mctechStmtResolverTestCase struct {
	shortDb     string // 当前数据库的短名
	sql         string // 传入的sql语句
	expectDBs   string // 期望检测到使用过的数据库列表（排序后）
	expectValue string // 期望的解析后的数据结构(一般属性)
	failure     string // 失败后抛出的异常信息
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
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*mctechStmtResolverTestCase{
		{"pf", "/*& tenant:gdcd */ /*& tenant:'gdcd' */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd1 */ select * from company", "", "", "多个 tenant hint包含不同的值: gdcd <=> gdcd1"},
		{"test", "describe company", "", "{{{,}{,{,false},[{mpp,allow}],{false,[],[]}}},test}", ""},
		{"test", "select * from company /*& global:true */", "test", "{{{,}{,{,false},[{mpp,allow}],{true,[],[]}}},test}", ""},
		//
		{"pf", "/*& global:true */ select * from company", "global_platform", "{{{,}{,{,false},[{mpp,allow}],{true,[],[]}}},global_platform}", ""},
		{"test", "/*& global:true */ select * from company", "test", "{{{,}{,{,false},[{mpp,allow}],{true,[],[]}}},test}", ""},
		{"pf", "/*& global:!ys2 */ select * from company", "global_platform", "{{{,}{,{,false},[{mpp,allow}],{true,[ys2],[]}}},global_platform}", ""},
		{"pf", "/*& global:-ys2 */ select * from company", "global_platform", "{{{,}{,{,false},[{mpp,allow}],{true,[ys2],[]}}},global_platform}", ""},
		{"pf", "/*& global:+ys2 */ select * from company", "global_platform", "{{{,}{,{,false},[{mpp,allow}],{true,[],[ys2]}}},global_platform}", ""},
		{"pf", "select * from company /*& global:-ys2,!ys3 */", "global_platform", "{{{,}{,{,false},[{mpp,allow}],{true,[ys2 ys3],[]}}},global_platform}", ""},
		{"pf", "select * from company /*& global:+ys2,+ys3 */", "global_platform", "{{{,}{,{,false},[{mpp,allow}],{true,[],[ys2 ys3]}}},global_platform}", ""},
		{"pf", "/*& global:!ys2,-ys3 , +ys22 , +ys33 */ select * from company", "global_platform", "{{{,}{,{,false},[{mpp,allow}],{true,[ys2 ys3],[ys22 ys33]}}},global_platform}", ""},
		// hint 格式不匹配
		{"pf", "/*  & global:true */ select * from company", "", "", "当前用户无法确定所属租户信息"},
		{"pf", "/* global:true */ select * from company", "", "", "当前用户无法确定所属租户信息"},
		{"test", "/* global:true */ select * from company", "test", "{{{,}{,{,false},[{mpp,allow}],{false,[],[]}}},test}", ""},
		{"pf", "/*& tenant:' */ select * from company", "", "", "\"tenant\" hint 值格式不正确 -> '"},
		{"pf", "/*& tenant:'gslq */ select * from company", "", "", "\"tenant\" hint 值格式不正确 -> 'gslq"},
		{"pf", "/*& tenant: '  gslq */ select * from company", "", "", "\"tenant\" hint 值格式不正确 -> '  gslq"},
		{"pf", "/*& tenant:gslq  ' */ select * from company", "", "", "\"tenant\" hint 值格式不正确 -> gslq  '"},
		// tenant hint
		{"pf", "/*& tenant:gdcd */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant:gdcd */ /*& global:1 */ select * from company", "", "", "存在tenant信息时，global不允许设置为true"},
		{"pf", "/*& tenant: gdcd */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant:'gdcd ' */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant:'  gdcd' */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant:'  gdcd   ' */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant:  '  gdcd   ' */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant: */ select * from company", "", "", "当前用户无法确定所属租户信息"},
		// 空值
		{"test", "/*& custom: */ select * from company", "test", "{{{,}{,{,false},[{custom,} {mpp,allow}],{false,[],[]}}},test}", ""},
		{"pf", "/*& tenant:'' */ select * from company", "", "", "当前用户无法确定所属租户信息"},
		{"pf", "/*& tenant:'    ' */ select * from company", "", "", "当前用户无法确定所属租户信息"},

		// request_id
		{"pf", "/*& tenant:gdcd */ /*& requestId:abc123456 */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {requestId,abc123456} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		// background
		{"pf", "/*& tenant:ztsj */ /*& background:true */ select * from company", "global_platform", "{{{,}{,{ztsj,false},[{background,true} {mpp,allow} {tenant,ztsj}],{false,[],[]}}},global_platform}", ""},
		// across
		{"pf", "/*& tenant:ztsj */ /*& across:global_cq3,global_ds */ select * from company", "global_platform", "{{{,}{,{ztsj,false},[{across,global_cq3|global_ds} {mpp,allow} {tenant,ztsj}],{false,[],[]}}},global_platform}", ""},
		// dbPrefix
		{"pd", "/*& dbPrefix:mock */ select * from company", "public_data", "{{{,}{mock,{,false},[{dbPrefix,mock} {mpp,allow}],{false,[],[]}}},public_data}", ""},
		// replace
		{"pd", "/*& $replace:tenant */ /*& tenant:gslq */ select * from company", "public_data", "{{{,}{,{gslq,false},[{mpp,allow} {tenant,gslq}],{false,[],[]}}},public_data}", ""},   // replace
		{"pd", "/*& $replace:tenant */ /*& tenant:'gslq' */ select * from company", "public_data", "{{{,}{,{gslq,false},[{mpp,allow} {tenant,gslq}],{false,[],[]}}},public_data}", ""}, // replace
		{"pd", "/*& $replace:tenant=mctech */ select * from company", "public_data", "{{{,}{,{,false},[{mpp,allow}],{false,[],[]}}},public_data}", ""},
		{"pd", "/*& $replace:tenant */ select * from company", "", "", "执行[replace]时未找到名称为'tenant'的参数的值"},

		// 新的值声明方式
		{"pf", "/*& tenant|gdcd */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/*& tenant|gdcd */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},

		// 租户隔离角色
		{"pf", "/*& impersonate: tenant */ select * from company", "", "", "impersonate的值错误。可选值为'tenant_only'"},
		{"pf", "/*& impersonate: tenant_only */ select * from company", "", "", "当前用户无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */"},
		{"pf", "/*& global:true */ /*& impersonate: tenant_only */ select * from company", "", "", "当前用户包含'租户隔离'角色，不允许启用 'global' hint"},
		{"pf", "/*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{,}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},

		// custom comment
		{"pf", "/* from:'demo-service' */ /*& tenant:gdcd */ /*& tenant:'gdcd' */ select * from company", "global_platform", "{{{demo-service,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/* from:'another-demo-service' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{another-demo-service,}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/* from:'demo-service.pf' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{demo-service.pf,}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/* from:'demo-service.pc' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{demo-service.pc,}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/* from:'another-demo-service.pf' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{another-demo-service.pf,}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/* package:'@mctech/dp-impala' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{,@mctech/dp-impala}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/* package:'@mctech/another-dp-impala' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{,@mctech/another-dp-impala}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
		{"pf", "/* from:'demo-service' */ /* package:'@mctech/dp-impala' */ /*& tenant|gdcd */ /*& impersonate: tenant_only */ select * from company", "global_platform", "{{{demo-service,@mctech/dp-impala}{,{gdcd,false},[{impersonate,tenant_only} {mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
	}

	doRunWithSessionTest(t, stmtResoverRunTestCase, cases)
}

func TestStmtResolverNormalizeDB(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/preps/SetSQLDBS",
		mock.M(t, "global_mtlp,global_qa,global_platform,global_cq3,global_mtlp,global_cq3"))
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/preps/SetSQLDBS")

	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*mctechStmtResolverTestCase{
		{"pf", "/*& tenant|gdcd */ select * from company", "global_cq3,global_mtlp,global_platform,global_qa", "{{{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}},global_platform}", ""},
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
	require.Equal(t, c.expectDBs, strings.Join(dbs, ","), c.Source(i))
	require.Equal(t, c.expectValue, info, c.Source(i))
	return nil
}
