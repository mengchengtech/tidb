package prapared

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/stretchr/testify/require"
)

type testStringFilterCase struct {
	pattern string
	target  string
	success bool
}

func (c *testStringFilterCase) Failure() string {
	return ""
}

func (c *testStringFilterCase) Source() any {
	return fmt.Sprintf("%s -> %s", c.pattern, c.target)
}

func TestStringFilter(t *testing.T) {
	cases := []*testStringFilterCase{
		{"starts-with:global_", "global_ipm", true},
		{"starts-with:global_", "___trans_db_global_ipm", false},
		{"global_platform", "global_platform", true},
		{"global_platform", "global_dwb", false},
		{"ends-with:_dw", "global_dw", true},
		{"ends-with:_dw_1", "global_dw", false},
		{"ends-with:_dw_1", "global_dw_1", true},
		{"ends-with:_dw_1", "global_dw_2", false},
		{"regex:.*_internal_.*", "gslq_internal_read", true},
		{"regex:.*_internal_.+", "gslq_internal_", false},
		{"contains:_internal_", "gslq_internal_write", true},
	}

	doRunTest(t, filterRunTestCase, cases)
}

type testDatabaseCheckerCase struct {
	tenant  string
	dbs     []string
	failure string
}

func (c *testDatabaseCheckerCase) Failure() string {
	return c.failure
}

func (c *testDatabaseCheckerCase) Source() any {
	return fmt.Sprintf("%s -> %v", c.tenant, c.dbs)
}

func newTestMCTechContext(tenant string) (mctech.MCTechContext, error) {
	result, err := mctech.NewResolveResult(tenant, map[string]any{
		"global": &mctech.GlobalValueInfo{},
	})
	context := mctech.NewBaseMCTechContext(result, nil)
	return context, err
}

func TestDatabaseChecker(t *testing.T) {
	cases := []*testDatabaseCheckerCase{
		// 当前租户信息不是来自账号所属角色
		{"", []string{"global_cq3", "global_mtlp"}, ""},
		// 当前租户信息来自账号所属角色
		{"gslq", []string{"global_platform", "global_ipm", "global_dw_1", "global_dw_2", "global_dwb"}, ""},     // 基础库，允许在一起使用
		{"gslq", []string{"global_platform", "global_cq3"}, ""},                                                 // 基础库，和一个普通库，允许在一起使用
		{"gslq", []string{"global_platform", "global_ipm", "global_cq3"}, ""},                                   // 基础库，和一个普通库，允许在一起使用
		{"gslq", []string{"global_platform", "global_ds", "global_cq3"}, "dbs not allow in the same statement"}, // 基础库，和两个普通库，不允许在一起使用
		{"gslq", []string{"global_ds", "global_mtlp"}, "dbs not allow in the same statement"},
		{"gslq", []string{"global_platform", "global_mtlp"}, ""},
		{"gslq", []string{"global_ma", "global_mtlp"}, ""},                    // 陕梦特殊要求，能在一起使用
		{"gslq", []string{"global_platform", "global_ma", "global_mtlp"}, ""}, // 陕梦特殊要求，能在一起使用
		{"gslq", []string{"global_platform", "global_mc", "global_ma", "global_mtlp"}, "dbs not allow in the same statement"},
		{"gslq", []string{"asset_component", "global_cq3"}, "dbs not allow in the same statement"},
		{"gslq", []string{"public_data", "global_cq3"}, "dbs not allow in the same statement"},
		{"gslq", []string{"public_XXXXX", "global_cq3"}, "dbs not allow in the same statement"},
	}

	doRunTest(t, checkRunTestCase, cases)
}

func checkRunTestCase(t *testing.T, c *testDatabaseCheckerCase) error {
	checker := NewMutexDatabaseCheckerWithParams(
		[]string{"starts-with:global_", "starts-with:asset_", "public_data"},
		nil,
		nil,
	)

	context, _ := newTestMCTechContext(c.tenant)
	return checker.Check(context, c.dbs)
}

func filterRunTestCase(t *testing.T, c *testStringFilterCase) error {
	p := NewStringFilter(c.pattern)
	success, err := p.Match(c.target)
	if err != nil {
		return err
	}
	require.Equal(t, c.success, success, c.Source())
	return nil
}
