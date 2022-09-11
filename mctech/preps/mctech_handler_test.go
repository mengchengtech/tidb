package preps

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/session"
	"github.com/stretchr/testify/require"
)

type handlerTestCase struct {
	sql              string
	expectChanged    bool
	tenantEnabled    bool
	dbCheckerEnabled bool
	failure          string
}

func (c *handlerTestCase) Failure() string {
	return c.failure
}

func (c *handlerTestCase) Source() any {
	return fmt.Sprintf("[%t,%t,%t] %s", c.expectChanged, c.tenantEnabled, c.dbCheckerEnabled, c.sql)
}

func TestHandler(t *testing.T) {
	cases := []*handlerTestCase{
		{"select * from company", false, false, true, ""},
		{"/*& tenant:gslq */ select * from company", false, false, true, ""},
		{"/*& global:true */ select * from company", false, false, true, ""},
		{"select * from company", false, false, false, ""},
		{"/*& tenant:gslq */ select * from company", true, true, false, ""},
		{"/*& $replace:tenant */ /*& tenant:gslq */ SELECT * FROM {{tenant}}_custom.{{tenant}}g6_progress_month_setting", true, true, false, ""},
		{"/*& global:1 */ select * from company", false, true, false, "存在tenant信息时，global不允许设置为true"},
		{"select * from global_cq3.company a join global_sq.table2 b on a.id = b.id", true, true, true, "dbs not allow in the same statement"},
	}

	doRunWithSessionTest(t, handlerRunTestCase, cases, "mock_write", "code_gslq", "code_gslq")
}

func handlerRunTestCase(t *testing.T, c *handlerTestCase, mctechCtx mctech.Context) (err error) {
	option := mctech.GetOption()
	tenantEnabled := option.TenantEnabled
	dbCheckerEnabled := option.DbCheckerEnabled
	option.TenantEnabled = c.tenantEnabled
	option.DbCheckerEnabled = c.dbCheckerEnabled

	defer func() {
		option.TenantEnabled = tenantEnabled
		option.DbCheckerEnabled = dbCheckerEnabled
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

	require.Equal(t, c.expectChanged, changed, c.Source())
	return nil
}
