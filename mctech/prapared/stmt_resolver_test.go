package prapared

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/session"
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
		{"pf", "/*& tenant:gdcd */ /*& tenant:gdcd */ select * from company", "", "发现多个tenant hint信息"},
		{"test", "describe company", "{{{,,false,[],{false,[]}}},test}", ""},
		{"test", "select * from company /*& global:true */", "{{{,,false,[],{true,[]}}},test}", ""},
		//
		{"pf", "/*& global:true */ select * from company", "{{{,,false,[],{true,[]}}},global_platform}", ""},
		{"test", "/*& global:true */ select * from company", "{{{,,false,[],{true,[]}}},test}", ""},
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
		// replace
		{"pd", "/*& $replace:tenant */ /*& tenant:gslq */ select * from company", "{{{,gslq,false,[{tenant,gslq}],{false,[]}}},public_data}", ""},   // replace
		{"pd", "/*& $replace:tenant */ /*& tenant:'gslq' */ select * from company", "{{{,gslq,false,[{tenant,gslq}],{false,[]}}},public_data}", ""}, // replace
		{"pd", "/*& $replace:tenant=mctech */ select * from company", "{{{,,false,[],{false,[]}}},public_data}", ""},
		{"pd", "/*& $replace:tenant */ select * from company", "", "执行[replace]时未找到名称为'tenant'的参数的值"},
	}

	doRunWithSessionTest(t, stmtResoverRunTestCase, cases, "root")
}

func stmtResoverRunTestCase(t *testing.T, c *mctechStmtResolverTestCase, session session.Session) error {
	resolver := &mctechStatementResolver{
		checker: getMutexDatabaseChecker(),
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
	if err != nil {
		return err
	}
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
	info := resolver.Context().GetInfo()
	require.Equal(t, c.expect, info, c.Source())
	return nil
}
