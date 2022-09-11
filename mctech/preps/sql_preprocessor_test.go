package preps

import (
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/stretchr/testify/require"
)

type preprocessorTestCase struct {
	sql          string
	actions      map[string]string
	params       map[string]any
	resultExpect string
	sqlExpect    string
	failure      string
}

func (c *preprocessorTestCase) Failure() string {
	return c.failure
}

func (c *preprocessorTestCase) Source() any {
	return c.sql
}

func TestProcessorWithRoot(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	// TODO: 完成单元测试
	cases := []*preprocessorTestCase{
		// global
		{"select * from company", nil, map[string]any{"global": "true"}, "{,,false,[],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2"}, "{,,false,[],{true,[ys2]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2,!ys3"}, "{,,false,[],{true,[ys2 ys3]}}", "", ""},
		// tenant hint
		{"select * from company", nil, map[string]any{"tenant": "gdcd"}, "{,gdcd,false,[{tenant,gdcd}],{false,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "global": "true"}, "", "", "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "requestId": "abc123456"}, "{,gdcd,false,[{requestId,abc123456} {tenant,gdcd}],{false,[]}}", "", ""},
		// background
		{"select * from company", nil, map[string]any{"tenant": "ztsj", "background": "true"}, "{,ztsj,false,[{background,true} {tenant,ztsj}],{false,[]}}", "", ""},
		// dbPrefix
		{"select * from company", nil, map[string]any{"dbPrefix": "mock"}, "{mock,,false,[{dbPrefix,mock}],{false,[]}}", "", ""},
		// action
		// $replace
		{"select * from custom.company", map[string]string{"nop": ""}, nil, "", "", "不支持的action操作"},
		{"select * from {{tenant}}_custom.company", map[string]string{"replace": "tenant=gslq"}, nil, "{,,false,[],{false,[]}}", "select * from gslq_custom.company", ""},
		{"select * from {{tenant}}_custom.company", map[string]string{"replace": "tenant"}, map[string]any{"tenant": "gdcd"}, "{,gdcd,false,[{tenant,gdcd}],{false,[]}}", "select * from gdcd_custom.company", ""},
		{"select * from {{tenant}}_custom.company", map[string]string{"replace": "tenant"}, nil, "", "", "执行[replace]时未找到名称为'tenant'的参数的值"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "root")
}

func TestPreprocessorWithGlobalAndTenentOnlyUser(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"global": "true"}, "{,,false,[],{true,[]}}", "", "当前数据库用户不允许启用 global hint"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "tenant_only")
}

func TestPreprocessorWithoutGlobalAndTenentOnlyUser(t *testing.T) {
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"global": "false"}, "{,,false,[],{false,[]}}", "", ""},
	}
	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "tenant_only")
}

func TestPreprocessorWithGlobalAndNotTenentOnlyUser(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"global": "true"}, "{,,false,[],{true,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "tenant_only1")
}

func TestPreprocessorTenentCodeConflict(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"tenant": "cr19g"}, "{,,false,[],{false,[]}}", "", "当前用户所属角色对应的租户信息与sql里传入的租户信息不一致. gslq (role) <=> cr19g (sql)"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "code_gslq")
}

func TestPreprocessorTenentCodeFromRole(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{}, "{,gslq,true,[],{false,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "code_gslq")
}

func TestPreprocessorWithTenentUser(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, nil, "{,gslq,true,[],{false,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "code_gslq")
}

func TestPreprocessorMultiRoleFailure(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, nil, "", "", "用户mock_write所属的角色存在多个租户的信息"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase,
		cases, "mock_write", "code_gslq", "code_gdcd")
}

func TestPreprocessorMultiRoleSuccess(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, nil, "{,gslq,true,[],{false,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases,
		"mock_write", "code_gslq", "code_gslq")
}

func preprocessorRunTestCase(t *testing.T, c *preprocessorTestCase, mctechCtx mctech.Context) error {
	processor := newSQLPreprocessor(c.sql)
	result, err := processor.Prepare(mctechCtx, c.actions, c.params)
	if err != nil {
		return err
	}

	require.Equal(t, c.resultExpect, result.String(), c.Source())
	outSQL := processor.preparedSQL
	require.NotContains(t, outSQL, "{{tenant}}", c.Source())
	if outSQL != c.sql {
		require.Equal(t, c.sqlExpect, outSQL, c.Source())
	}
	return nil
}
