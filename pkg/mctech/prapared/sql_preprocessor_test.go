package prapared

import (
	"testing"

	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
)

type preprocessorTestCase struct {
	sql          string
	actions      map[string]string
	params       map[string]any
	resultExpect map[string]any
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
		{"select * from company", nil, map[string]any{"global": "true"}, map[string]any{"global": map[string]any{"set": true}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2"}, map[string]any{"global": map[string]any{"set": true, "excludes": []string{"ys2"}}}, "", ""},
		{"select * from company", nil, map[string]any{"global": "!ys2,!ys3"}, map[string]any{"global": map[string]any{"set": true, "excludes": []string{"ys2", "ys3"}}}, "", ""},
		// tenant hint
		{"select * from company", nil, map[string]any{"tenant": "gdcd"}, map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd"}}, "", ""},
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "global": "true"}, nil, "", "存在tenant信息时，global不允许设置为true"},

		// request_id
		{"select * from company", nil, map[string]any{"tenant": "gdcd", "requestId": "abc123456"}, map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd", "requestId": "abc123456"}}, "", ""},
		// background
		{"select * from company", nil, map[string]any{"tenant": "ztsj", "background": "true"}, map[string]any{"tenant": "ztsj", "params": map[string]any{"background": "true", "tenant": "ztsj"}}, "", ""},
		// dbPrefix
		{"select * from company", nil, map[string]any{"dbPrefix": "mock"}, map[string]any{"prefix": "mock", "params": map[string]any{"dbPrefix": "mock"}}, "", ""},
		// action
		// $replace
		{"select * from custom.company", map[string]string{"nop": ""}, nil, nil, "", "不支持的action操作"},
		{"select * from {{tenant}}_custom.company", map[string]string{"replace": "tenant=gslq"}, nil, map[string]any{}, "select * from gslq_custom.company", ""},
		{"select * from {{tenant}}_custom.company", map[string]string{"replace": "tenant"}, map[string]any{"tenant": "gdcd"}, map[string]any{"tenant": "gdcd", "params": map[string]any{"tenant": "gdcd"}}, "select * from gdcd_custom.company", ""},
		{"select * from {{tenant}}_custom.company", map[string]string{"replace": "tenant"}, nil, nil, "", "执行[replace]时未找到名称为'tenant'的参数的值"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "root")
}

func TestPreprocessorWithInternalUser(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, nil, map[string]any{"tenant": "gslq", "tenantFromRole": true}, "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases, "mock_write", "gslq_internal_write")
}

func TestPreprocessorMultiRoleFailure(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, nil, nil, "", "用户mock_write所属的角色存在多个租户的信息"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase,
		cases, "mock_write", "gslq_internal_write", "gdcd_internal_write")
}

func TestPreprocessorMultiRoleFailure2(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, nil, nil, "", "当前用户mock_write同时属于多种类型的角色"},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase,
		cases, "mock_write", "gslq_internal_write", "mc_dev_write")
}

func TestPreprocessorMultiRoleSuccess(t *testing.T) {
	// {{{dbPrefix,tenant,tenantFromRole,[params],{global,excludes}}},currentDb}
	cases := []*preprocessorTestCase{
		{"select * from company", nil, nil, map[string]any{"tenant": "gslq", "tenantFromRole": true}, "", ""},
	}

	doRunWithSessionTest(t, preprocessorRunTestCase, cases,
		"mock_write", "gslq_internal_write", "gslq_internal_write")
}

func preprocessorRunTestCase(t *testing.T, c *preprocessorTestCase, session session.Session) error {
	processor := newSQLPreprocessor(c.sql)
	result, err := processor.Prepare(session, c.actions, c.params)
	if err != nil {
		return err
	}

	require.Equal(t, c.resultExpect, result.GetInfoForTest(), c.Source())
	outSQL := processor.preparedSQL
	require.NotContains(t, outSQL, "{{tenant}}", c.Source())
	if outSQL != c.sql {
		require.Equal(t, c.sqlExpect, outSQL, c.Source())
	}
	return nil
}
