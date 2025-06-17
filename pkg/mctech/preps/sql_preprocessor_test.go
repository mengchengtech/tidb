package preps_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/preps"
	"github.com/stretchr/testify/require"
)

type preprocessorTestCase struct {
	sql          string
	actions      []preps.ActionInfo
	params       map[string]any
	comments     map[string]string
	roles        []string
	resultExpect map[string]any
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
	// {service: "", prefix:"", tenant:"", tenantFromRole: true, params:{tenant:{code:"",fromRole:true}, mpp: "", global:{set:true, excludes: [""], "includes": [""]}, comments:{}}}
	cases := []*preprocessorTestCase{
		// global
		{"select * from company", nil, map[string]any{"global": "true"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2"}}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "-ys2"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2"}}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "+ys2"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "includes": []string{"ys2"}}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2,-ys3"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "+ys2,+ys3"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "includes": []string{"ys2", "ys3"}}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2,-ys3 , +ys22 , +ys33"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}, "includes": []string{"ys22", "ys33"}}, "comments": map[string]any{}}, "", ""},
		// tenant hint
		{"select * from company", nil, map[string]any{"tenant": "gdcd"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "global": "true"}, nil, nil, nil, "", "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "requestId": "abc123456"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "requestId": "abc123456", "mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		// background
		{"select * from company", nil, map[string]any{"tenant": "ztsj", "background": "true"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "ztsj", "fromRole": false}, "params": map[string]any{"background": "true", "tenant": "ztsj", "mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		// dbPrefix
		{"select * from company", nil, map[string]any{"dbPrefix": "mock"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "prefix": "mock", "params": map[string]any{"dbPrefix": "mock", "mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		// action
		// $replace
		{"select * from custom.company", preps.NewActionsForTest(map[string]string{"nop": ""}), map[string]any{}, nil, nil, nil, "", "不支持的action操作"},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant=gslq"}), map[string]any{}, nil, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "comments": map[string]any{}}, "select * from gslq_custom.company", ""},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant"}), map[string]any{"tenant": "gdcd"}, nil, nil, map[string]any{"tenant": map[string]any{"code": "gdcd", "fromRole": false}, "params": map[string]any{"tenant": "gdcd", "mpp": "allow"}, "comments": map[string]any{}}, "select * from gdcd_custom.company", ""},
		{"select * from {{tenant}}_custom.company", preps.NewActionsForTest(map[string]string{"replace": "tenant"}), map[string]any{}, nil, nil, nil, "", "执行[replace]时未找到名称为'tenant'的参数的值"},
		// custom comment
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"service": "demo-service"}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "another-demo-service"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"service": "another-demo-service"}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service.pf"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"service": "demo-service.pf"}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service.pc"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"service": "demo-service.pc"}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "another-demo-service.pf"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"service": "another-demo-service.pf"}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"package": "@mctech/dp-impala"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"pkg": "@mctech/dp-impala"}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"package": "@mctech/another-dp-impala"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"pkg": "@mctech/another-dp-impala"}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]string{"from": "demo-service", "package": "@mctech/dp-impala"}, nil, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{"service": "demo-service", "pkg": "@mctech/dp-impala"}}, "", ""},
		// tenant role
		// TestPreprocessorWithGlobalAndTenentOnlyUser
		{"select * from company", nil, map[string]any{"global": "true"}, nil, []string{"tenant_only"}, nil, "", "当前用户包含'租户隔离'角色，不允许启用 'global' hint"},
		// TestPreprocessorWithoutGlobalAndTenentOnlyUser
		{"select * from company", nil, map[string]any{"global": "false"}, nil, []string{"tenant_only"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		// TestPreprocessorWithGlobalAndNotTenentOnlyUser
		{"select * from company", nil, map[string]any{"global": "true"}, nil, []string{"tenant_only1"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "global": map[string]any{"set": true}, "comments": map[string]any{}}, "", ""},
		// TestPreprocessorTenentCodeConflict
		{"select * from company", nil, map[string]any{"tenant": "cr19g"}, nil, []string{"code_gslq"}, nil, "", "当前用户所属角色对应的租户信息与sql里传入的租户信息不一致. gslq (role) <=> cr19g (sql)"},
		// TestPreprocessorTenentCodeFromRole
		{"select * from company", nil, map[string]any{}, nil, []string{"code_gslq"}, map[string]any{"tenant": map[string]any{"code": "gslq", "fromRole": true}, "params": map[string]any{"mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		// TestPreprocessorMultiRoleFailure
		{"select * from company", nil, map[string]any{}, nil, []string{"code_gslq", "code_gdcd"}, nil, "", "当前用户所属的角色存在多个租户的信息"},
		// TestPreprocessorMultiRoleSuccess
		{"select * from company", nil, map[string]any{}, nil, []string{"code_gslq", "code_gslq"}, map[string]any{"tenant": map[string]any{"code": "gslq", "fromRole": true}, "params": map[string]any{"mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		// tenant_omit
		{"select * from company", nil, map[string]any{}, nil, []string{"tenant_omit"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{}, nil, []string{"tenant_omit", "code_gslq"}, nil, "", "当前用户不允许同时包含'租户隔离'和'忽略租户'角色"},
		{"select * from company", nil, map[string]any{"tenant": "gslq"}, nil, []string{"tenant_omit"}, map[string]any{"tenant": map[string]any{"code": "", "fromRole": false}, "params": map[string]any{"tenant": "gslq", "mpp": "allow"}, "comments": map[string]any{}}, "", ""},
		{"select * from company", nil, map[string]any{}, nil, []string{"tenant_only", "tenant_omit"}, nil, "", "当前用户不允许同时包含'租户隔离'和'忽略租户'角色"},
		{"select * from company", nil, map[string]any{"impersonate": "tenant_only"}, nil, []string{"tenant_omit"}, nil, "", "当前用户不允许同时包含'租户隔离'和'忽略租户'角色"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases)
}

func preprocessorRunTestCase(t *testing.T, i int, c *preprocessorTestCase, mctechCtx mctech.Context) error {
	processor := preps.NewSQLPreprocessorForTest(c.sql)
	comments := mctech.NewComments(c.comments[mctech.CommentFrom], c.comments[mctech.CommentPackage])
	result, err := processor.Parse(mctechCtx, c.actions, comments, c.params)
	if err != nil {
		return err
	}

	require.Equal(t, c.resultExpect, result.GetInfoForTest(), c.Source(i))
	outSQL := processor.GetParsedSQL()
	require.NotContains(t, outSQL, "{{tenant}}", c.Source(i))
	if outSQL != c.sql {
		require.Equal(t, c.sqlExpect, outSQL, c.Source(i))
	}
	return nil
}
