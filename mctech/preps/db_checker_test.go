package preps_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/mctech/preps"
	mcworker "github.com/pingcap/tidb/mctech/worker"
	"github.com/pingcap/tidb/sessionctx"
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

func TestDatabaseCheckerFromCrossDBManager(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/worker/get-cross-db-info",
		mock.M(t, []map[string]any{
			{"Service": "demo-service", "Groups": []any{"global_ec3,global_ma", "global_cq5,global_mtlp"}},
			{"Package": "demo-package", "Groups": []any{"global_ec3,global_mtlp", "global_ec3,global_ec"}},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/worker/get-cross-db-info")

	doRunTest(t, checkRunTestCase, []*testDatabaseCheckerCase{
		// 指定的服务可以访问给定的数据库列表
		{true, map[string]string{"from": "demo-service"}, "", []string{"global_cq5", "global_mtlp"}, ""},
		{true, map[string]string{"from": "demo-service"}, "", []string{"global_ec", "global_mtlp"}, "dbs not allow in the same statement"},
		{true, map[string]string{"from": "demo-forbidden-service"}, "", []string{"global_cq5", "global_mtlp"}, "dbs not allow in the same statement"},
		// 指定的包可以访问给定的数据库列表
		{true, map[string]string{"package": "demo-package"}, "", []string{"global_ec3", "global_mtlp"}, ""},
		{true, map[string]string{"package": "demo-package"}, "", []string{"global_ec", "global_mtlp"}, "dbs not allow in the same statement"},
		{true, map[string]string{"package": "demo-forbidden-package"}, "", []string{"global_ec3", "global_mtlp"}, "dbs not allow in the same statement"},
	})
}

func TestDatabaseCheckerFromCrossDBManagerAnyName(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/worker/get-cross-db-info",
		mock.M(t, []map[string]any{
			{"Service": mcworker.MatchAnyInvoker, "Groups": []any{"global_ec3,global_mtlp", "global_cq5,global_mtlp"}},
			{"Package": mcworker.MatchAnyInvoker, "Groups": []any{"global_ec3,global_mtlp", "global_ec3,global_ma"}},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/worker/get-cross-db-info")

	doRunTest(t, checkRunTestCase, []*testDatabaseCheckerCase{
		// 没有任何自定义hint信息
		{true, nil, "", []string{"global_cq5", "global_mtlp"}, ""},
		// 所有服务可以跨库访问指定的数据库列表
		{true, map[string]string{"from": "demo-service"}, "", []string{"global_cq5", "global_mtlp"}, ""},
		{true, map[string]string{"from": "demo-other-service"}, "", []string{"global_cq5", "global_mtlp"}, ""},
		{true, map[string]string{"from": "demo-forbidden-service"}, "", []string{"global_ec", "global_mtlp"}, "dbs not allow in the same statement"},
		// 所有包可以跨库访问指定的数据库列表
		{true, map[string]string{"package": "@mctech/demo-package"}, "", []string{"global_ec3", "global_mtlp"}, ""},
		{true, map[string]string{"package": "@mctech/demo-other-package"}, "", []string{"global_ec3", "global_mtlp"}, ""},
		{true, map[string]string{"package": "@mctech/demo-forbidden-package"}, "", []string{"global_ec", "global_mtlp"}, "dbs not allow in the same statement"},
	})
}

func TestDatabaseCheckerFromCrossDBManagerAllowAllDBsAndAnyName(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/worker/get-cross-db-info",
		mock.M(t, []map[string]any{
			{"Service": mcworker.MatchAnyInvoker, "AllowAllDBs": true},
			{"Package": mcworker.MatchAnyInvoker, "AllowAllDBs": true},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/worker/get-cross-db-info")

	doRunTest(t, checkRunTestCase, []*testDatabaseCheckerCase{
		// 所有服务或包可以跨库访问所有数据库都
		{true, map[string]string{"from": "demo-service"}, "", []string{"global_cq5", "global_mtlp"}, ""},
		{true, map[string]string{"from": "demo-other-service"}, "", []string{"global_cq5", "global_mtlp"}, ""},
		{true, map[string]string{"from": "demo-third-service"}, "", []string{"global_platform", "global_mtlp"}, ""},
		// 所有服务或包可以跨库访问所有数据库都
		{true, map[string]string{"package": "@mctech/demo-package"}, "", []string{"global_ec3", "global_mtlp"}, ""},
		{true, map[string]string{"package": "@mctech/demo-other-package"}, "", []string{"global_ec3", "global_mtlp"}, ""},
		{true, map[string]string{"package": "@mctech/demo-third-package"}, "", []string{"global_platform", "global_mtlp"}, ""},
	})
}

func TestDatabaseCheckerFromInternalConfig(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/session/mctech-ddl-upgrade", mock.M(t, "false"))
	defer failpoint.Disable("github.com/pingcap/tidb/session/mctech-ddl-upgrade")

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
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]any{
			"DbChecker.Excepts": []string{"demo-service", "another-demo-service.pf", "@mctech/dp-impala"},
		}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")

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

func checkRunTestCase(t *testing.T, i int, c *testDatabaseCheckerCase, sctx sessionctx.Context) error {
	option := config.GetMCTechConfig()
	checker := preps.NewMutuallyExclusiveDatabaseCheckerWithParamsForTest(
		option.DbChecker.Mutex,
		option.DbChecker.Exclude)

	roles, err := preps.NewFlagRoles(c.tenantOnly, false, true)
	if err != nil {
		return err
	}
	comments := mctech.NewComments(c.comments[mctech.CommentFrom], c.comments[mctech.CommentPackage])
	result, err := mctech.NewParseResult("gslq", roles, comments, map[string]any{
		"global": mctech.NewGlobalValue(false, nil, nil),
		"across": c.across,
	})
	if err != nil {
		panic(err)
	}
	mctx, _ := mctech.WithNewContext(sctx)
	modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetParseResult(result)
	return checker.Check(mctx, &mockStmtTextAware{}, mctech.StmtSchemaInfo{Databases: c.dbs})
}

func filterRunTestCase(t *testing.T, i int, c *testStringFilterCase, _ sessionctx.Context) error {
	p, ok := mctech.NewStringFilter(c.pattern)
	require.True(t, ok)
	success := p.Match(c.target)
	require.Equal(t, c.success, success, c.Source(i))
	return nil
}
