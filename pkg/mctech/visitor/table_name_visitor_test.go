package visitor_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/visitor"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"
)

type tenantTableNameVisitorTestCase struct {
	shortDb string
	sql     string
	expect  string
}

func (t tenantTableNameVisitorTestCase) Source() any {
	return t.sql
}

func (t *tenantTableNameVisitorTestCase) Expect() string {
	return t.expect
}

func (t *tenantTableNameVisitorTestCase) Failure() string {
	return ""
}

var dbsCases = []*tenantTableNameVisitorTestCase{
	{"pf", "select * from global_mtlp.g_statistic_category", "global_mtlp.g_statistic_category"},
	{"pf", "select * from global_dw.component", "global_dw_1.component"},
	{"pf", "select * from component", "global_platform.component"},
	{"pf", "with category as (select * from global_sq.material_category) select * from category", "global_sq.material_category"},
	{"pf", "with category as (select * from global_sq.material_category) select * from other_category o inner join category a on o.id = a.id", "global_platform.other_category|global_sq.material_category"},

	// 同一个with里前面的cte用到的表名与后面的cte名称重名，前一个cte里用到的表需要当作普通表
	{"pf", "with category as (select * from global_ipm.material_category where creator_id in (select id from user)),user as (select * from awesome_ai.wechat_user) select * from category", "awesome_ai.wechat_user|global_ipm.material_category|global_platform.user"},
	// 同一个with里后面的cte用到的表名与前面的cte名称重名，后一个cte里用到的表需要当作cte表
	{"pf", "with user as (select * from awesome_ai.wechat_user),category as (select * from global_ipm.material_category where creator_id in (select id from user)) select * from category", "awesome_ai.wechat_user|global_ipm.material_category"},
}

func TestGetDbs(t *testing.T) {
	doRunTest(t, doRunTenantTableNameVisitorTestCase, dbsCases)
}

func doRunTenantTableNameVisitorTestCase(t *testing.T, c *tenantTableNameVisitorTestCase) error {
	p := parser.New()
	stmts, _, err := p.Parse(c.sql, "", "")
	require.NoErrorf(t, err, "source %v", c.sql)
	var schema mctech.StmtSchemaInfo
	context, err := newTestMCTechContext(dbMap[c.shortDb], "", false, nil, nil)
	require.NoError(t, err)

	if schema, _, err = visitor.ApplyExtension(context, stmts[0], "", ""); err != nil {
		require.NoError(t, err)
	}
	list := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		list = append(list, fmt.Sprintf("%s.%s", table.Database, table.Table))
	}
	require.Equalf(t, c.expect, strings.Join(list, "|"), "restore %v; expects %v", c.sql, c.expect)
	return nil
}
