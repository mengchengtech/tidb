package preps

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/session"
	"github.com/stretchr/testify/require"
)

type handlerTestCase struct {
	sql              string
	expectChanged    bool
	tenantEnabled    bool
	dbCheckerEnabled bool
	dbs              []string
	failure          string
}

func (c *handlerTestCase) Failure() string {
	return c.failure
}

func (c *handlerTestCase) Source() any {
	return fmt.Sprintf("[%t,%t,%t] %s", c.expectChanged, c.tenantEnabled, c.dbCheckerEnabled, c.sql)
}

func TestHandlerWithTenantDisable(t *testing.T) {
	cases := []*handlerTestCase{
		{"select * from information_schema.analyze_status", false, false, false, nil, ""},
		{"select * from company", false, false, true, nil, ""},
		{"/*& tenant:gslq */ select * from company", false, false, true, nil, ""},
		{"/*& global:true */ select * from company", false, false, true, nil, ""},
		{"select * from company", false, false, false, nil, ""},
	}
	doRunWithSessionTest(t, handlerRunTestCase, cases, "mock_write", "code_gslq", "code_gslq")
}

func TestHandlerWithTenantEnable(t *testing.T) {
	cases := []*handlerTestCase{
		{"select * from information_schema.analyze_status", true, true, false, []string{"information_schema"}, ""},
		{"/*& tenant:gslq */ select * from company", true, true, false, []string{"test"}, ""},
		{"/*& $replace:tenant */ /*& $replace:p1=zzz */  /*& tenant:gslq */ SELECT * FROM {{tenant}}_custom.{{tenant}}g6_progress_month_setting where name='{{p1}}'", true, true, false, []string{"gslq_custom"}, ""},
		{"/*& global:1 */ select * from company", false, true, false, []string{"test"}, "存在tenant信息时，global不允许设置为true"},
		{"select * from global_cq3.company a join global_sq.table2 b on a.id = b.id", true, true, true, []string{"global_cq3", "global_sq"}, "dbs not allow in the same statement"},
	}

	doRunWithSessionTest(t, handlerRunTestCase, cases, "mock_write", "code_gslq", "code_gslq")
}

func handlerRunTestCase(t *testing.T, c *handlerTestCase, mctechCtx mctech.Context) (err error) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/GetMctechOption",
		mctech.M(t, map[string]bool{"TenantEnabled": c.tenantEnabled, "DbCheckerEnabled": c.dbCheckerEnabled}),
	)

	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/mctech/GetMctechOption")
	}()
	var sql string
	if sql, err = handler.PrepareSQL(mctechCtx, c.sql); err != nil {
		return err
	}
	session := mctechCtx.Session().(session.Session)
	stmts, err := session.Parse(context.Background(), sql)
	if err != nil {
		return err
	}

	require.Equal(t, 1, len(stmts), c.Source())
	changed, err := handler.ApplyAndCheck(mctechCtx, stmts)
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		dbs := mctechCtx.GetDbs(stmt)
		require.Equal(t, len(c.dbs), len(dbs))
		for _, db := range dbs {
			require.Contains(t, dbs, db, c.Source())
		}
	}
	require.Equal(t, c.expectChanged, changed, c.Source())
	return nil
}
