package preps

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/mock"
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
		{"global_*", "global_ipm", true},
		{"global_*", "___trans_db_global_ipm", false},
		{"global_platform", "global_platform", true},
		{"global_platform", "global_dwb", false},
		{"*_dw", "global_dw", true},
		{"*_dw_1", "global_dw", false},
		{"*_dw_1", "global_dw_1", true},
		{"*_dw_1", "global_dw_2", false},
		{"*_tenant_*", "gslq_tenant_read", true},
		{"_tenant_", "gslq_tenant_write", true},
	}

	doRunTest(t, filterRunTestCase, cases)
}

type testDatabaseCheckerCase struct {
	tenantOnly bool
	dbs        []string
	failure    string
}

func (c *testDatabaseCheckerCase) Failure() string {
	return c.failure
}

func (c *testDatabaseCheckerCase) Source() any {
	return fmt.Sprintf("%t -> %v", c.tenantOnly, c.dbs)
}

func newTestMCTechContext(tenantOnly bool) (mctech.Context, error) {
	result, err := mctech.NewPrepareResult("gslq", tenantOnly, map[string]any{
		"global": &mctech.GlobalValueInfo{},
	})
	context := mctech.NewBaseContext(false)
	context.(mctech.ModifyContext).SetPrepareResult(result)
	return context, err
}

func TestDatabaseChecker(t *testing.T) {
	cases := []*testDatabaseCheckerCase{
		// 当前账号不属于tenant_only角色
		{false, []string{"global_cq3", "global_mtlp"}, ""},
		{false, []string{"global_mp", "global_mp"}, ""},
		// 当前账号属于tenant_only角色
		{true, []string{"global_platform", "global_ipm", "global_dw_1", "global_dw_2", "global_dwb"}, ""},     // 基础库，允许在一起使用
		{true, []string{"global_platform", "global_cq3"}, ""},                                                 // 基础库，和一个普通库，允许在一起使用
		{true, []string{"global_platform", "global_ipm", "global_cq3"}, ""},                                   // 基础库，和一个普通库，允许在一起使用
		{true, []string{"global_platform", "global_ds", "global_cq3"}, "dbs not allow in the same statement"}, // 基础库，和两个普通库，不允许在一起使用
		{true, []string{"global_ds", "global_mtlp"}, "dbs not allow in the same statement"},
		{true, []string{"global_platform", "global_mtlp"}, ""},
		{true, []string{"global_cq3", "global_sq"}, "dbs not allow in the same statement"},
		{true, []string{"global_ma", "global_mtlp"}, ""},                    // 陕梦特殊要求，能在一起使用
		{true, []string{"global_platform", "global_ma", "global_mtlp"}, ""}, // 陕梦特殊要求，能在一起使用
		{true, []string{"global_platform", "global_mc", "global_ma", "global_mtlp"}, "dbs not allow in the same statement"},
		{true, []string{"asset_component", "global_cq3"}, "dbs not allow in the same statement"},
		{true, []string{"global_mp", "global_mp"}, ""},
	}
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"DbChecker.Compatible": false}),
	)
	doRunTest(t, checkRunTestCase, cases)
	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}

type mockStmtTextAware struct{}

func (a *mockStmtTextAware) OriginalText() string {
	return "mock original text"
}

func checkRunTestCase(t *testing.T, c *testDatabaseCheckerCase) error {
	option := config.GetMCTechConfig()
	checker := newMutexDatabaseCheckerWithParams(
		option.DbChecker.Mutex,
		option.DbChecker.Exclude,
		option.DbChecker.Across)

	context, _ := newTestMCTechContext(c.tenantOnly)
	return checker.Check(context, &mockStmtTextAware{}, c.dbs)
}

func filterRunTestCase(t *testing.T, c *testStringFilterCase) error {
	p, ok := mctech.NewStringFilter(c.pattern)
	require.True(t, ok)
	success := p.Match(c.target)
	require.Equal(t, c.success, success, c.Source())
	return nil
}
