package preps_test

import (
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/preps"
	"github.com/stretchr/testify/require"
)

type preprocessorTestCase struct {
	sql          string
	actions      []preps.ActionInfo
	params       map[string]any
	comments     map[string]string
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
		{"select * from company", nil, map[string]any{"global": "!ys2"}, nil, "{,}{,{,false},[{mpp,allow}],{true,[ys2]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2,!ys3"}, nil, "{,}{,{,false},[{mpp,allow}],{true,[ys2 ys3]}}", "", ""},
		// tenant hint
		{"select * from company", nil, map[string]any{"tenant": "gdcd"}, nil, "{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "global": "true"}, nil, "", "", "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "requestId": "abc123456"}, nil, "{,}{,{gdcd,false},[{mpp,allow} {requestId,abc123456} {tenant,gdcd}],{false,[]}}", "", ""},
		// background
		{"select * from company", nil, map[string]any{"tenant": "ztsj", "background": "true"}, nil, "{,}{,{ztsj,false},[{background,true} {mpp,allow} {tenant,ztsj}],{false,[]}}", "", ""},
		// dbPrefix
		{"select * from company", nil, map[string]any{"dbPrefix": "mock"}, nil, "{,}{mock,{,false},[{dbPrefix,mock} {mpp,allow}],{false,[]}}", "", ""},
		// action
		// $replace
		{"select * from custom.company", preps.NewActionsForTest(map[string]string{"nop": ""}), map[string]any{}, nil, "", "", "不支持的action操作"},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant=gslq"}), map[string]any{}, nil, "{,}{,{,false},[{mpp,allow}],{false,[]}}", "select * from gslq_custom.company", ""},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant"}), map[string]any{"tenant": "gdcd"}, nil, "{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[]}}", "select * from gdcd_custom.company", ""},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant"}), map[string]any{}, nil, "", "", "执行[replace]时未找到名称为'tenant'的参数的值"},
		// custom comment
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service"}, "{demo-service,}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "another-demo-service"}, "{another-demo-service,}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service.pf"}, "{demo-service.pf,}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service.pc"}, "{demo-service.pc,}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "another-demo-service.pf"}, "{another-demo-service.pf,}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"package": "@mctech/dp-impala"}, "{,@mctech/dp-impala}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"package": "@mctech/another-dp-impala"}, "{,@mctech/another-dp-impala}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service", "package": "@mctech/dp-impala"}, "{demo-service,@mctech/dp-impala}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "root")
}

func TestPreprocessorWithGlobalAndTenentOnlyUser(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"global": "true"}, nil, "{,}{,{,false},[],{true,[]}}", "", "当前数据库用户包含租户隔离角色，不允许启用 global hint"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "tenant_only")
}

func TestPreprocessorWithoutGlobalAndTenentOnlyUser(t *testing.T) {
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"global": "false"}, nil, "{,}{,{,false},[{mpp,allow}],{false,[]}}", "", ""},
	}
	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "tenant_only")
}

func TestPreprocessorWithGlobalAndNotTenentOnlyUser(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"global": "true"}, nil, "{,}{,{,false},[{mpp,allow}],{true,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "tenant_only1")
}

func TestPreprocessorTenentCodeConflict(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{"tenant": "cr19g"}, nil, "{,}{,{,false},[],{false,[]}}", "", "当前用户所属角色对应的租户信息与sql里传入的租户信息不一致. gslq (role) <=> cr19g (sql)"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "code_gslq")
}

func TestPreprocessorTenentCodeFromRole(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{}, nil, "{,}{,{gslq,true},[{mpp,allow}],{false,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "code_gslq")
}

func TestPreprocessorWithTenentUser(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{}, nil, "{,}{,{gslq,true},[{mpp,allow}],{false,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "code_gslq")
}

func TestPreprocessorMultiRoleFailure(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{}, nil, "", "", "用户mock_write所属的角色存在多个租户的信息"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase,
		cases, "mock_write", "code_gslq", "code_gdcd")
}

func TestPreprocessorMultiRoleSuccess(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, map[string]any{}, nil, "{,}{,{gslq,true},[{mpp,allow}],{false,[]}}", "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases,
		"mock_write", "code_gslq", "code_gslq")
}

func preprocessorRunTestCase(t *testing.T, c *preprocessorTestCase, mctechCtx mctech.Context) error {
	processor := preps.NewSQLPreprocessorForTest(c.sql)
	comments := preps.NewComments(c.comments[mctech.CommentFrom], c.comments[mctech.CommentPackage])
	result, err := processor.Prepare(mctechCtx, c.actions, comments, c.params)
	if err != nil {
		return err
	}

	require.Equal(t, c.resultExpect, result.String(), c.Source())
	outSQL := processor.GetPreparedSQL()
	require.NotContains(t, outSQL, "{{tenant}}", c.Source())
	if outSQL != c.sql {
		require.Equal(t, c.sqlExpect, outSQL, c.Source())
	}
	return nil
}
