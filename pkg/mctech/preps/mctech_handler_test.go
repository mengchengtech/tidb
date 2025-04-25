package preps_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	_ "github.com/pingcap/tidb/pkg/mctech/preps"
	"github.com/pingcap/tidb/pkg/session/types"
	"github.com/stretchr/testify/require"
)

var handler = mctech.GetHandler()

type handlerTestCase struct {
	sql              string
	roles            []string
	expectChanged    bool
	tenantEnabled    bool
	dbCheckerEnabled bool
	dbs              []string
	failure          string
}

func (c *handlerTestCase) Failure() string {
	return c.failure
}

func (c *handlerTestCase) Source(i int) any {
	return fmt.Sprintf("(%d) [%t,%t,%t] %s", i, c.expectChanged, c.tenantEnabled, c.dbCheckerEnabled, c.sql)
}

func (c *handlerTestCase) Roles() []string {
	return c.roles
}

func TestHandler(t *testing.T) {
	cases := []*handlerTestCase{
		// TestHandlerWithTenantDisable
		{"select * from information_schema.analyze_status", []string{"code_gslq", "code_gslq"}, false, false, false, nil, ""},
		{"select * from company", []string{"code_gslq", "code_gslq"}, false, false, true, nil, ""},
		{"/*& tenant:gslq */ select * from company", []string{"code_gslq", "code_gslq"}, false, false, true, nil, ""},
		{"/*& global:true */ select * from company", []string{"code_gslq", "code_gslq"}, false, false, true, nil, ""},
		{"select * from company", []string{"code_gslq", "code_gslq"}, false, false, false, nil, ""},

		// TestHandlerWithTenantEnableAndTenantRole
		{"select * from information_schema.analyze_status", []string{"code_gslq", "code_gslq"}, true, true, false, []string{"information_schema"}, ""},
		{"/*& tenant:gslq */ select * from company", []string{"code_gslq", "code_gslq"}, true, true, false, []string{"test"}, ""},
		{"/*& $replace:tenant */ /*& $replace:p1=zzz */  /*& tenant:gslq */ SELECT * FROM {{tenant}}_custom.{{tenant}}g6_progress_month_setting where name='{{p1}}'", []string{"code_gslq", "code_gslq"}, true, true, false, []string{"gslq_custom"}, ""},
		{"/*& global:1 */ select * from company", []string{"code_gslq", "code_gslq"}, false, true, false, []string{"test"}, "存在tenant信息时，global不允许设置为true"},
		{"select * from global_cq3.company a join global_sq.table2 b on a.id = b.id", []string{"code_gslq", "code_gslq"}, true, true, true, []string{"global_cq3", "global_sq"}, "dbs not allow in the same statement"},
		// TestHandlerWithTenantEnableAndNoTenantRole
		{"select * from global_platform.company", nil, true, true, false, []string{"test"}, "当前用户无法确定所属租户信息"},
	}

	doRunWithSessionTest(t, handlerRunTestCase, cases)
}

func handlerRunTestCase(t *testing.T, i int, c *handlerTestCase, mctechCtx mctech.Context) (err error) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.Enabled": c.tenantEnabled, "DbChecker.Enabled": c.dbCheckerEnabled}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	var sql string
	if sql, err = handler.PrepareSQL(mctechCtx, c.sql); err != nil {
		return err
	}
	session := mctechCtx.Session().(types.Session)
	stmts, err := session.Parse(context.Background(), sql)
	if err != nil {
		return err
	}

	require.Equal(t, 1, len(stmts), c.Source(i))
	var changed bool
	for _, stmt := range stmts {
		ch, err := handler.ApplyAndCheck(mctechCtx, stmt)
		if err != nil {
			return err
		}

		changed = changed || ch
	}

	for _, stmt := range stmts {
		dbs := mctechCtx.GetDbs(stmt)
		require.Equal(t, len(c.dbs), len(dbs))
		for _, db := range dbs {
			require.Contains(t, dbs, db, c.Source(i))
		}
	}
	require.Equal(t, c.expectChanged, changed, c.Source(i))
	return nil
}
