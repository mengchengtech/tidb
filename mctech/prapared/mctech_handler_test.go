package prapared

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/session"
	"github.com/stretchr/testify/require"
)

type mctechHandlerTestCase struct {
	sql              string
	expectChanged    bool
	tenantEnabled    bool
	dbCheckerEnabled bool
	failure          string
}

func (c *mctechHandlerTestCase) Failure() string {
	return c.failure
}

func (c *mctechHandlerTestCase) Source() any {
	return fmt.Sprintf("[%t,%t,%t] %s", c.expectChanged, c.tenantEnabled, c.dbCheckerEnabled, c.sql)
}

func TestMCTechHandler(t *testing.T) {
	cases := []*mctechHandlerTestCase{
		{"select * from company", false, false, true, ""},
		{"/*& tenant:gslq */ select * from company", false, false, true, ""},
		{"/*& global:true */ select * from company", false, false, true, ""},
		{"select * from company", false, false, false, ""},
		{"/*& tenant:gslq */ select * from company", true, true, false, ""},
		{"/*& $replace:tenant */ /*& tenant:cr19 */ SELECT * FROM {{tenant}}_custom.{{tenant}}g6_progress_month_setting", true, true, false, ""},
		{"/*& global:1 */ select * from company", false, true, false, "存在tenant信息时，global不允许设置为true"},
		{"select * from global_cq3.company a join global_sq.table2 b on a.id = b.id", true, true, true, "dbs not allow in the same statement"},
	}

	doRunWithSessionTest(t, mctechHandlerRunTestCase, cases,
		"mock_write", "gslq_internal_write", "gslq_internal_write")
}

func mctechHandlerRunTestCase(t *testing.T, c *mctechHandlerTestCase, session session.Session) (err error) {
	option := mctech.GetOption()
	mctech.SetOptionForTest(&mctech.Option{
		TenantEnabled:    c.tenantEnabled,
		DbCheckerEnabled: c.dbCheckerEnabled,
	})
	defer mctech.SetOptionForTest(option)
	handler := CreateMCTechHandler(session, c.sql)
	var sql string
	if sql, err = handler.PrapareSQL(); err != nil {
		return err
	}

	stmts, err := session.Parse(context.Background(), sql)
	if err != nil {
		return err
	}

	require.Equal(t, 1, len(stmts), c.Source())
	changed, err := handler.ApplyAndCheck(stmts)
	if err != nil {
		return err
	}

	require.Equal(t, c.expectChanged, changed, c.Source())
	return nil
}
