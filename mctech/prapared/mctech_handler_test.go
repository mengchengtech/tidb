package prapared

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

	doRunWithSessionTest(t, handlerRunTestCase, cases,
		"mock_write", "gslq_tenant_only_write", "gslq_tenant_only_write")
}

func handlerRunTestCase(t *testing.T, c *handlerTestCase, session session.Session) (err error) {
	option := mctech.GetOption()
	mctech.SetOptionForTest(&mctech.Option{
		TenantEnabled:    c.tenantEnabled,
		DbCheckerEnabled: c.dbCheckerEnabled,
	})
	defer mctech.SetOptionForTest(option)
	handler := GetHandlerFactory().CreateHandler()
	var sql string
	if sql, err = handler.PrapareSQL(session, c.sql); err != nil {
		return err
	}

	stmts, err := session.Parse(context.Background(), sql)
	if err != nil {
		return err
	}

	require.Equal(t, 1, len(stmts), c.Source())
	changed, err := handler.ApplyAndCheck(session, stmts)
	if err != nil {
		return err
	}

	require.Equal(t, c.expectChanged, changed, c.Source())
	return nil
}
