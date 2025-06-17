package preps_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/mctech/preps"
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

func (c *testStringFilterCase) Source(i int) any {
	return fmt.Sprintf("(%d) %s -> %s", i, c.pattern, c.target)
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
	comments   map[string]string
	across     string
	dbs        []string
	failure    string
}

func (c *testDatabaseCheckerCase) Failure() string {
	return c.failure
}

func (c *testDatabaseCheckerCase) Source(i int) any {
	return fmt.Sprintf("(%d) %t -> %v", i, c.tenantOnly, c.dbs)
}

func newTestMCTechContext(roles mctech.FlagRoles, comments mctech.Comments, across string) (mctech.Context, error) {
	result, err := mctech.NewParseResult("gslq", roles, comments, map[string]any{
		"global": mctech.NewGlobalValue(false, nil, nil),
		"across": across,
	})
	context := mctech.NewBaseContext(false)
	context.(mctech.ModifyContext).SetParseResult(result)
	return context, err
}

func TestDatabaseChecker(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]any{
			"DbChecker.Across": []string{"global_mtlp|global_ma", "global_cq3|global_qa"},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	cases := []*testDatabaseCheckerCase{
		// 当前账号不属于tenant_only角色
		{false, nil, "", []string{"global_cq3", "global_mtlp"}, ""},
		{false, nil, "", []string{"global_mp", "global_mp"}, ""},
		// 当前账号属于tenant_only角色
		{true, nil, "", []string{"global_platform", "global_ipm", "global_dw_1", "global_dw_2", "global_dwb"}, ""},     // 基础库，允许在一起使用
		{true, nil, "", []string{"global_platform", "global_cq3"}, ""},                                                 // 基础库，和一个普通库，允许在一起使用
		{true, nil, "", []string{"global_platform", "global_ipm", "global_cq3"}, ""},                                   // 基础库，和一个普通库，允许在一起使用
		{true, nil, "", []string{"global_platform", "global_ds", "global_cq3"}, "dbs not allow in the same statement"}, // 基础库，和两个普通库，不允许在一起使用
		{true, nil, "", []string{"global_ds", "global_mtlp"}, "dbs not allow in the same statement"},
		{true, nil, "", []string{"global_platform", "global_mtlp"}, ""},
		{true, nil, "", []string{"global_cq3", "global_sq"}, "dbs not allow in the same statement"},
		{true, nil, "", []string{"global_ma", "global_mtlp"}, ""},                    // 陕梦特殊要求，能在一起使用
		{true, nil, "", []string{"global_platform", "global_ma", "global_mtlp"}, ""}, // 陕梦特殊要求，能在一起使用
		{true, nil, "", []string{"global_platform", "global_mc", "global_ma", "global_mtlp"}, "dbs not allow in the same statement"},
		{true, nil, "", []string{"asset_component", "global_cq3"}, "dbs not allow in the same statement"},
		{true, nil, "", []string{"global_mp", "global_mp"}, ""},

		{true, nil, "global_ds|global_ds", []string{"global_ds"}, ""},
		{true, nil, "global_ds|global_ds", []string{"global_ds", "global_mtlp"}, "dbs not allow in the same statement"},
		{true, nil, "global_ds|global_mtlp", []string{"global_ds", "global_mtlp"}, ""},
		{true, nil, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds"}, ""},
		{true, nil, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa"}, ""},
		{true, nil, "global_ds|global_qa", []string{"global_sq", "global_ds", "global_qa"}, "dbs not allow in the same statement"},
		{true, nil, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, "dbs not allow in the same statement"},
	}
	doRunTest(t, checkRunTestCase, cases)
}

func TestDatabaseCheckerUseCustomComment(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]any{
			"DbChecker.Excepts": []string{"demo-service", "another-demo-service.pf", "@mctech/dp-impala"},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	cases := []*testDatabaseCheckerCase{
		// custom comment crossdb check pass
		{true, map[string]string{"from": "demo-service"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, ""},
		{true, map[string]string{"from": "demo-service", "package": "@mctech/dp-impala"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, ""},
		{true, map[string]string{"from": "another-demo-service.pf", "package": "@mctech/dp-impala"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, ""},
		{true, map[string]string{"package": "@mctech/dp-impala"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, ""},
		{true, map[string]string{"from": "another-demo-service", "package": "@mctech/dp-impala"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, ""},
		{true, map[string]string{"from": "another-demo-service.pf", "package": "@mctech/another-dp-impala"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, ""},
		{true, map[string]string{"from": "another-demo-service.pf"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, ""},
		// custom comment crossdb check unpass
		{true, map[string]string{"from": "another-demo-service"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, "dbs not allow in the same statement"},
		{true, map[string]string{"package": "@mctech/another-dp-impala"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, "dbs not allow in the same statement"},
		{true, map[string]string{"from": "another-demo-service", "package": "@mctech/another-dp-impala"}, "global_ds|global_qa|global_sq", []string{"global_sq", "global_ds", "global_qa", "global_mb"}, "dbs not allow in the same statement"},
	}
	doRunTest(t, checkRunTestCase, cases)
}

type mockStmtTextAware struct{}

func (a *mockStmtTextAware) OriginalText() string {
	return "mock original text"
}

func checkRunTestCase(t *testing.T, i int, c *testDatabaseCheckerCase) error {
	option := config.GetMCTechConfig()
	checker := preps.NewMutexDatabaseCheckerWithParamsForTest(
		option.DbChecker.Mutex,
		option.DbChecker.Exclude,
		option.DbChecker.Across)

	roles, err := preps.NewFlagRoles(c.tenantOnly, false, true)
	if err != nil {
		return err
	}
	comments := mctech.NewComments(c.comments[mctech.CommentFrom], c.comments[mctech.CommentPackage])
	context, _ := newTestMCTechContext(roles, comments, c.across)
	return checker.Check(context, &mockStmtTextAware{}, c.dbs)
}

func filterRunTestCase(t *testing.T, i int, c *testStringFilterCase) error {
	p, ok := mctech.NewStringFilter(c.pattern)
	require.True(t, ok)
	success := p.Match(c.target)
	require.Equal(t, c.success, success, c.Source(i))
	return nil
}
