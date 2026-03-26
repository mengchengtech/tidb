package interceptor_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestCerberusIntegration(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	failpoint.Enable("github.com/pingcap/tidb/mctech/MockMctechHttp",
		mock.M(t, map[string]any{"DWIndex.Current": map[string]any{"current": 1}}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
		failpoint.Disable("github.com/pingcap/tidb/mctech/MockMctechHttp")
	}()

	cases := []struct {
		sql      string
		roles    []string
		cerberus bool
		errMsg   string
	}{
		// tenant_only without cerberus proxy
		{"select * from {{tenant}}_custom.t11", []string{"tenant_only"}, false, `near "{tenant}}_custom.t11`},
		{"select * from global_dw.t12", []string{"tenant_only"}, false, "当前用户无法确定所属租户信息"},
		{"select * from global_ipm.t13", []string{"tenant_only"}, false, "当前用户无法确定所属租户信息"},
		{"/*& tenant:gslq */ /*& $replace:tenant*/ select * from {{tenant}}_custom.t11", []string{"tenant_only"}, false, ""},
		{"/*& tenant:gslq */ select * from global_dw.t12", []string{"tenant_only"}, false, ""},
		{"/*& tenant:gslq */ select * from global_ipm.t13", []string{"tenant_only"}, false, ""},
		{"/*& global:true */ select * from global_ipm.t13", []string{"tenant_only"}, false, "当前用户包含'租户隔离'角色，不允许启用 'global' hint"},

		// tenant_only with tenant_only role and cerberus proxy
		{"/*& tenant:gslq */ /*& $replace:tenant*/ select * from {{tenant}}_custom.t11", []string{"tenant_only"}, true, `near "{tenant}}_custom.t11`},
		{"select * from global_dw.t12", []string{"tenant_only", "code_gdcd"}, true, "Table 'global_dw.t12' doesn't exist"},
		{"select * from global_ipm.t13", []string{"tenant_only", "code_gdcd"}, true, ""},
		{"/*& global:true */ select * from global_ipm.t13", []string{"tenant_only"}, true, ""},
	}

	for index, cs := range cases {
		store := testkit.CreateMockStore(t)
		tk := initDbAndData(t, store)
		initDBForCerberusProxyTest(tk)
		sessVars := tk.Session().GetSessionVars()
		for _, role := range cs.roles {
			sessVars.ActiveRoles = append(sessVars.ActiveRoles, &auth.RoleIdentity{Username: role})
		}
		tk.Session().GetSessionVars().CerburusProxy = cs.cerberus
		_, err := tk.Exec(cs.sql)
		if err != nil {
			if cs.errMsg == "" {
				// 不期望出现异常，但是出现了
				require.NoErrorf(t, err, "[%d] -> sql: %s, roles: %v, error: %v", index, cs.sql, cs.roles, err)
			} else {
				// 期望出现特定的异常
				require.ErrorContainsf(t, err, cs.errMsg, "[%d] -> sql: %s, roles: %v, error: %v", index, cs.sql, cs.roles, err)
			}
		} else {
			if cs.errMsg != "" {
				// 期望出现异常，但是没有
				require.Errorf(t, err, "[%d] -> expected error for sql: %s, roles: %v, error: %s", index, cs.sql, cs.roles, cs.errMsg)
			}
		}
	}
}

func initDBForCerberusProxyTest(tk *testkit.TestKit) {
	tk.MustExec("create database gslq_custom")
	tk.MustExec("create database global_dw_1")
	tk.MustExec("create database global_ipm")

	tk.MustExec("create table gslq_custom.t11 (id int)")
	tk.MustExec("create table global_dw_1.t12 (id int, tenant varchar(50))")
	tk.MustExec("create table global_ipm.t13 (id int, tenant varchar(50))")
}
