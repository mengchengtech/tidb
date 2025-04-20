package preps_test

import (
	"fmt"
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
	roles        []string
	resultExpect string
	sqlExpect    string
	failure      string
}

func (c *preprocessorTestCase) Failure() string {
	return c.failure
}

func (c *preprocessorTestCase) Source(i int) any {
	return fmt.Sprintf("(%d) %s", i, c.sql)
}

func (c *preprocessorTestCase) Roles() []string {
	return c.roles
}

func TestProcessor(t *testing.T) {
	// {{{service,dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	// TODO: 完成单元测试
	cases := []*preprocessorTestCase{
		// global
		{"select * from company", nil, map[string]any{"global": "true"}, nil, nil, "{,}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2"}, nil, nil, "{,}{,{,false},[{mpp,allow}],{true,[ys2],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "-ys2"}, nil, nil, "{,}{,{,false},[{mpp,allow}],{true,[ys2],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "+ys2"}, nil, nil, "{,}{,{,false},[{mpp,allow}],{true,[],[ys2]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2, -ys3"}, nil, nil, "{,}{,{,false},[{mpp,allow}],{true,[ys2 ys3],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "+ys2, +ys3"}, nil, nil, "{,}{,{,false},[{mpp,allow}],{true,[],[ys2 ys3]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2,-ys3 , +ys22 , +ys33"}, nil, nil, "{,}{,{,false},[{mpp,allow}],{true,[ys2 ys3],[ys22 ys33]}}", "", ""},
		// tenant hint
		{"select * from company", nil, map[string]any{"tenant": "gdcd"}, nil, nil, "{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "global": "true"}, nil, nil, "", "", "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "requestId": "abc123456"}, nil, nil, "{,}{,{gdcd,false},[{mpp,allow} {requestId,abc123456} {tenant,gdcd}],{false,[],[]}}", "", ""},
		// background
		{"select * from company", nil, map[string]any{"tenant": "ztsj", "background": "true"}, nil, nil, "{,}{,{ztsj,false},[{background,true} {mpp,allow} {tenant,ztsj}],{false,[],[]}}", "", ""},
		// dbPrefix
		{"select * from company", nil, map[string]any{"dbPrefix": "mock"}, nil, nil, "{,}{mock,{,false},[{dbPrefix,mock} {mpp,allow}],{false,[],[]}}", "", ""},
		// action
		// $replace
		{"select * from custom.company", preps.NewActionsForTest(map[string]string{"nop": ""}), map[string]any{}, nil, nil, "", "", "不支持的action操作"},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant=gslq"}), map[string]any{}, nil, nil, "{,}{,{,false},[{mpp,allow}],{false,[],[]}}", "select * from gslq_custom.company", ""},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant"}), map[string]any{"tenant": "gdcd"}, nil, nil, "{,}{,{gdcd,false},[{mpp,allow} {tenant,gdcd}],{false,[],[]}}", "select * from gdcd_custom.company", ""},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant"}), map[string]any{}, nil, nil, "", "", "执行[replace]时未找到名称为'tenant'的参数的值"},
		// custom comment
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service"}, nil, "{demo-service,}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "another-demo-service"}, nil, "{another-demo-service,}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service.pf"}, nil, "{demo-service.pf,}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service.pc"}, nil, "{demo-service.pc,}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "another-demo-service.pf"}, nil, "{another-demo-service.pf,}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"package": "@mctech/dp-impala"}, nil, "{,@mctech/dp-impala}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"package": "@mctech/another-dp-impala"}, nil, "{,@mctech/another-dp-impala}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service", "package": "@mctech/dp-impala"}, nil, "{demo-service,@mctech/dp-impala}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		// tenant role
		// TestPreprocessorWithGlobalAndTenentOnlyUser
		{"select * from company", nil, map[string]any{"global": "true"}, nil, []string{"tenant_only"}, "", "", "当前用户包含'租户隔离'角色，不允许启用 'global' hint"},
		// TestPreprocessorWithoutGlobalAndTenentOnlyUser
		{"select * from company", nil, map[string]any{"global": "false"}, nil, []string{"tenant_only"}, "{,}{,{,false},[{mpp,allow}],{false,[],[]}}", "", ""},
		// TestPreprocessorWithGlobalAndNotTenentOnlyUser
		{"select * from company", nil, map[string]any{"global": "true"}, nil, []string{"tenant_only1"}, "{,}{,{,false},[{mpp,allow}],{true,[],[]}}", "", ""},
		// TestPreprocessorTenentCodeConflict
		{"select * from company", nil, map[string]any{"tenant": "cr19g"}, nil, []string{"code_gslq"}, "", "", "当前用户所属角色对应的租户信息与sql里传入的租户信息不一致. gslq (role) <=> cr19g (sql)"},
		// TestPreprocessorTenentCodeFromRole
		{"select * from company", nil, map[string]any{}, nil, []string{"code_gslq"}, "{,}{,{gslq,true},[{mpp,allow}],{false,[],[]}}", "", ""},
		// TestPreprocessorMultiRoleFailure
		{"select * from company", nil, map[string]any{}, nil, []string{"code_gslq", "code_gdcd"}, "", "", "当前用户所属的角色存在多个租户的信息"},
		// TestPreprocessorMultiRoleSuccess
		{"select * from company", nil, map[string]any{}, nil, []string{"code_gslq", "code_gslq"}, "{,}{,{gslq,true},[{mpp,allow}],{false,[],[]}}", "", ""},
		// tenant_omit
		{"select * from company", nil, map[string]any{}, nil, []string{"tenant_omit"}, "{,}{,{,false},[{mpp,allow}],{false,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{}, nil, []string{"tenant_omit", "code_gslq"}, "", "", "当前用户不允许同时包含'租户隔离'和'忽略租户'角色"},
		{"select * from company", nil, map[string]any{"tenant": "gslq"}, nil, []string{"tenant_omit"}, "{,}{,{,false},[{mpp,allow} {tenant,gslq}],{false,[],[]}}", "", ""},
		{"select * from company", nil, map[string]any{}, nil, []string{"tenant_only", "tenant_omit"}, "", "", "当前用户不允许同时包含'租户隔离'和'忽略租户'角色"},
		{"select * from company", nil, map[string]any{"impersonate": "tenant_only"}, nil, []string{"tenant_omit"}, "", "", "当前用户不允许同时包含'租户隔离'和'忽略租户'角色"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases)
}

func preprocessorRunTestCase(t *testing.T, i int, c *preprocessorTestCase, mctechCtx mctech.Context) error {
	processor := preps.NewSQLPreprocessorForTest(c.sql)
	comments := preps.NewComments(c.comments[mctech.CommentFrom], c.comments[mctech.CommentPackage])
	result, err := processor.Prepare(mctechCtx, c.actions, comments, c.params)
	if err != nil {
		return err
	}

	require.Equal(t, c.resultExpect, result.String(), c.Source(i))
	outSQL := processor.GetPreparedSQL()
	require.NotContains(t, outSQL, "{{tenant}}", c.Source(i))
	if outSQL != c.sql {
		require.Equal(t, c.sqlExpect, outSQL, c.Source(i))
	}
	return nil
}
