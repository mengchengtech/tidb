package visitor_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/mctech/visitor"
	"github.com/pingcap/tidb/parser"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type tenantDatabaseNameVisitorTestCase struct {
	shortDb string
	sql     string
	expect  string
}

func (t tenantDatabaseNameVisitorTestCase) Source() any {
	return t.sql
}

func (t *tenantDatabaseNameVisitorTestCase) Expect() string {
	return t.expect
}

func (t *tenantDatabaseNameVisitorTestCase) Failure() string {
	return ""
}

var dbsCases = []*tenantDatabaseNameVisitorTestCase{
	{"pf", "select * from global_mtlp.g_statistic_category", "global_mtlp"},
	{"pf", "select * from global_dw.component", "global_dw_1"},
	{"pf", "select * from component", "global_platform"},
	{"pf", "with category as (select * from global_sq.material_category) select * from category", "global_sq"},
	{"pf", "with category as (select * from global_sq.material_category) select * from other_category o inner join category a on o.id = a.id", "global_platform|global_sq"},

	// 同一个with里前面的cte用到的表名与后面的cte名称重名，前一个cte里用到的表需要当作普通表
	{"pf", "with category as (select * from global_ipm.material_category where creator_id in (select id from user)),user as (select * from awesome_ai.wechat_user) select * from category", "awesome_ai|global_ipm|global_platform"},
	// 同一个with里后面的cte用到的表名与前面的cte名称重名，后一个cte里用到的表需要当作cte表
	{"pf", "with user as (select * from awesome_ai.wechat_user),category as (select * from global_ipm.material_category where creator_id in (select id from user)) select * from category", "awesome_ai|global_ipm"},
}

func TestGetDbs(t *testing.T) {
	doRunTest(t, doRunTenantDatabaseNameVisitorTestCase, dbsCases)
}

func doRunTenantDatabaseNameVisitorTestCase(t *testing.T, c *tenantDatabaseNameVisitorTestCase) error {
	p := parser.New()
	stmts, _, err := p.Parse(c.sql, "", "")
	require.NoErrorf(t, err, "source %v", c.sql)
	var dbs []string
	context, err := newTestMCTechContext(dbMap[c.shortDb], "", false, nil, nil)
	require.NoError(t, err)

	if dbs, _, err = visitor.ApplyExtension(context, stmts[0], "", ""); err != nil {
		require.NoError(t, err)
	}
	slices.Sort(dbs)
	require.Equalf(t, c.expect, strings.Join(dbs, "|"), "restore %v; expects %v", c.sql, c.expect)
	return nil
}
