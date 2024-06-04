package preps_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/preps"
	"github.com/stretchr/testify/require"
)

type mctechStmtResolverTestCase struct {
	shortDb string
	sql     string
	expect  string
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
		{"pf", "/*& tenant:gdcd */ /*& tenant:'gdcd' */ select * from company", "{{{,gdcd,false,[{mpp,allow} {tenant,gdcd}],{false,[]}}},global_platform}", ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd */ select * from company", "{{{,gdcd,false,[{mpp,allow} {tenant,gdcd}],{false,[]}}},global_platform}", ""},
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd1 */ select * from company", "", "多个 tenant hint包含不同的值: gdcd <=> gdcd1"},
		{"test", "describe company", "{{{,,false,[{mpp,allow}],{false,[]}}},test}", ""},
		{"test", "select * from company /*& global:true */", "{{{,,false,[{mpp,allow}],{true,[]}}},test}", ""},
		//
		{"pf", "/*& global:true */ select * from company", "{{{,,false,[{mpp,allow}],{true,[]}}},global_platform}", ""},
		{"test", "/*& global:true */ select * from company", "{{{,,false,[{mpp,allow}],{true,[]}}},test}", ""},
		{"pf", "/*& global:!ys2 */ select * from company", "{{{,,false,[{mpp,allow}],{true,[ys2]}}},global_platform}", ""},
		{"pf", "select * from company /*& global:!ys2,!ys3 */", "{{{,,false,[{mpp,allow}],{true,[ys2 ys3]}}},global_platform}", ""},
		// hint 格式不匹配
		{"pf", "/*  & global:true */ select * from company", "", "当前用户root无法确定所属租户信息"},
		{"pf", "/* global:true */ select * from company", "", "当前用户root无法确定所属租户信息"},
		{"test", "/* global:true */ select * from company", "{{{,,false,[{mpp,allow}],{false,[]}}},test}", ""},
		// tenant hint
		{"pf", "/*& tenant:gdcd */ select * from company", "{{{,gdcd,false,[{mpp,allow} {tenant,gdcd}],{false,[]}}},global_platform}", ""},
		{"pf", "/*& tenant:gdcd */ /*& global:1 */ select * from company", "", "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"pf", "/*& tenant:gdcd */ /*& requestId:abc123456 */ select * from company", "{{{,gdcd,false,[{mpp,allow} {requestId,abc123456} {tenant,gdcd}],{false,[]}}},global_platform}", ""},
		// background
		{"pf", "/*& tenant:ztsj */ /*& background:true */ select * from company", "{{{,ztsj,false,[{background,true} {mpp,allow} {tenant,ztsj}],{false,[]}}},global_platform}", ""},
		// dbPrefix
		{"pd", "/*& dbPrefix:mock */ select * from company", "{{{mock,,false,[{dbPrefix,mock} {mpp,allow}],{false,[]}}},public_data}", ""},
		// replace
		{"pd", "/*& $replace:tenant */ /*& tenant:gslq */ select * from company", "{{{,gslq,false,[{mpp,allow} {tenant,gslq}],{false,[]}}},public_data}", ""},   // replace
		{"pd", "/*& $replace:tenant */ /*& tenant:'gslq' */ select * from company", "{{{,gslq,false,[{mpp,allow} {tenant,gslq}],{false,[]}}},public_data}", ""}, // replace
		{"pd", "/*& $replace:tenant=mctech */ select * from company", "{{{,,false,[{mpp,allow}],{false,[]}}},public_data}", ""},
		{"pd", "/*& $replace:tenant */ select * from company", "", "执行[replace]时未找到名称为'tenant'的参数的值"},

		// 新的值声明方式
		{"pf", "/*& tenant|gdcd */ select * from company", "{{{,gdcd,false,[{mpp,allow} {tenant,gdcd}],{false,[]}}},global_platform}", ""},
		{"pf", "/*& tenant|gdcd */ select * from company", "{{{,gdcd,false,[{mpp,allow} {tenant,gdcd}],{false,[]}}},global_platform}", ""},
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
	require.Equal(t, c.expect, info, c.Source())
	return nil
}
