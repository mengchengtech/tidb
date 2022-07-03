package tenant

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser"
	. "github.com/pingcap/tidb/parser/format"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/stretchr/testify/require"
)

var dbMap = map[string]string{
	"pf": "global_platform",
	"dw": "global_dw",
}

type mctechTestCase struct {
	shortDb string
	src     string
	expect  string
}

type testMCTechContext struct {
	mctech.MCTechContext
	currentDb string
}

func (d *testMCTechContext) GetInfo() string {
	info := d.MCTechContext.GetInfo()
	return fmt.Sprintf("{%s,%s}", info, d.CurrentDB())
}

func (d *testMCTechContext) CurrentDB() string {
	return d.currentDb
}

type testDBSelector struct {
	dbIndex mctech.DbIndex
}

func (s *testDBSelector) GetDbIndex() (mctech.DbIndex, error) {
	return s.dbIndex, nil
}

func newTestMCTechContext(currentDb string) (mctech.MCTechContext, error) {
	result, err := mctech.NewResolveResult("gslq4dev", map[string]any{
		"dbPrefix": "mock",
		"global":   &mctech.GlobalValueInfo{Global: false},
	})

	if err != nil {
		return nil, err
	}

	context := &testMCTechContext{
		MCTechContext: mctech.NewBaseMCTechContext(
			result, &testDBSelector{dbIndex: 1}),
	}

	context.currentDb, err = context.ToPhysicalDbName(currentDb)
	return context, err
}

func doRunTest(t *testing.T, cases []mctechTestCase, enableWindowFunc bool) {
	p := parser.New()
	p.EnableWindowFunc(true)
	for _, tbl := range cases {
		stmts, _, err := p.Parse(tbl.src, "", "")
		require.NoErrorf(t, err, "source %v", tbl.src)
		var sb strings.Builder
		p := parser.New()
		p.EnableWindowFunc(enableWindowFunc)
		comment := fmt.Sprintf("source %v", tbl.src)
		restoreSQLs := ""
		for _, stmt := range stmts {
			sb.Reset()
			context, err := newTestMCTechContext(dbMap[tbl.shortDb])
			require.NoError(t, err, comment)
			visitor := NewTenantVisitor(context, "", "")
			stmt.Accept(visitor)
			err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreBracketAroundBinaryOperation, &sb))
			require.NoError(t, err, comment)
			restoreSQL := sb.String()
			comment = fmt.Sprintf("source %v; restore %v", tbl.src, restoreSQL)
			if restoreSQLs != "" {
				restoreSQLs += "; "
			}
			restoreSQLs += restoreSQL

		}
		require.Equalf(t, tbl.expect, restoreSQLs, "restore %v; expect %v", restoreSQLs, tbl.expect)
	}
}

var deleteSingleTableCases = []mctechTestCase{
	{"pf", "delete from org_relation_temp", "DELETE FROM `org_relation_temp` WHERE (`org_relation_temp`.`tenant`='gslq4dev')"},
	{"pf", "delete from component where id > 100", "DELETE FROM `component` WHERE ((`component`.`tenant`='gslq4dev') AND (`id`>100))"},
	{"pf", "delete a from component a join component_param as b on a.id = b.component_id", "DELETE `a` FROM `component` AS `a` JOIN `component_param` AS `b` ON ((`b`.`tenant`='gslq4dev') AND (`a`.`id`=`b`.`component_id`)) WHERE (`a`.`tenant`='gslq4dev')"},
}

var deleteMultipleTableCases = []mctechTestCase{
	{"pf", "delete a, b from component as a inner join component_param as b inner join component_param_detail as c where a.id = b.id and b.id = c.id", "DELETE `a`,`b` FROM (`component` AS `a` JOIN `component_param` AS `b`) JOIN `component_param_detail` AS `c` WHERE ((((`a`.`tenant`='gslq4dev') AND (`b`.`tenant`='gslq4dev')) AND (`c`.`tenant`='gslq4dev')) AND ((`a`.`id`=`b`.`id`) AND (`b`.`id`=`c`.`id`)))"},
	{"pf", "delete from a, b using component as a inner join component_param as b inner join component_param_detail as c where a.id = b.id and b.id = c.id", "DELETE FROM `a`,`b` USING (`component` AS `a` JOIN `component_param` AS `b`) JOIN `component_param_detail` AS `c` WHERE ((((`a`.`tenant`='gslq4dev') AND (`b`.`tenant`='gslq4dev')) AND (`c`.`tenant`='gslq4dev')) AND ((`a`.`id`=`b`.`id`) AND (`b`.`id`=`c`.`id`)))"},
	{"pf", "delete a from component as a left join component_param as b on a.id = b.id where b.id is null", "DELETE `a` FROM `component` AS `a` LEFT JOIN `component_param` AS `b` ON ((`b`.`tenant`='gslq4dev') AND (`a`.`id`=`b`.`id`)) WHERE ((`a`.`tenant`='gslq4dev') AND `b`.`id` IS NULL)"},
}

var deleteWithCTECases = []mctechTestCase{
	{"pf", "with tmp as (select * from component_param as b where b.component_id is not null) delete from component where id in (select id from Tmp)", "WITH `tmp` AS (SELECT * FROM `component_param` AS `b` WHERE ((`b`.`tenant`='gslq4dev') AND `b`.`component_id` IS NOT NULL)) DELETE FROM `component` WHERE ((`component`.`tenant`='gslq4dev') AND `id` IN (SELECT `id` FROM `Tmp`))"},
	{"pf", "with tmp as (select * from component_param as b where b.component_id is not null) delete a from component a join Tmp b on a.id = b.id", "WITH `tmp` AS (SELECT * FROM `component_param` AS `b` WHERE ((`b`.`tenant`='gslq4dev') AND `b`.`component_id` IS NOT NULL)) DELETE `a` FROM `component` AS `a` JOIN `Tmp` AS `b` ON (`a`.`id`=`b`.`id`) WHERE (`a`.`tenant`='gslq4dev')"},
}

// ----------- grant --------------------
// {"grant all on database public_data to role gslq4dev_internal_read", "GRANT ALL ON DATABASE public_data TO ROLE gslq4dev_internal_read"},

var insertIntoSelectCases = []mctechTestCase{
	{"pf", "insert into component (id, name) select id, name from global_ipm.component", "INSERT INTO `component` (`id`,`name`,`tenant`) SELECT `id`,`name`,'gslq4dev' AS `tenant` FROM `mock_global_ipm`.`component` WHERE (`mock_global_ipm`.`component`.`tenant`='gslq4dev')"},
	{"pf", "insert into component (id, name, tenant) select id, name, 'gslq4dev' from global_ipm.component", "INSERT INTO `component` (`id`,`name`,`tenant`) SELECT `id`,`name`,_UTF8MB4'gslq4dev' FROM `mock_global_ipm`.`component` WHERE (`mock_global_ipm`.`component`.`tenant`='gslq4dev')"},
	{"pf", "insert into component (id, name, tenant) select id, name, 'gslq4dev' AS `TT` from global_ipm.component", "INSERT INTO `component` (`id`,`name`,`tenant`) SELECT `id`,`name`,_UTF8MB4'gslq4dev' AS `TT` FROM `mock_global_ipm`.`component` WHERE (`mock_global_ipm`.`component`.`tenant`='gslq4dev')"},
	{"pf", "insert into component (id, name) select id, name from component", "INSERT INTO `component` (`id`,`name`,`tenant`) SELECT `id`,`name`,'gslq4dev' AS `tenant` FROM `component` WHERE (`component`.`tenant`='gslq4dev')"},
	{"pf", "insert into component (id, name) select id, name from component as c", "INSERT INTO `component` (`id`,`name`,`tenant`) SELECT `id`,`name`,'gslq4dev' AS `tenant` FROM `component` AS `c` WHERE (`c`.`tenant`='gslq4dev')"},
	{"pf", "insert into component (id, name) select id, name from component", "INSERT INTO `component` (`id`,`name`,`tenant`) SELECT `id`,`name`,'gslq4dev' AS `tenant` FROM `component` WHERE (`component`.`tenant`='gslq4dev')"},
}

var insertIntoValuesCases = []mctechTestCase{
	{"pf", "insert into component (id, name) values(1, 'zhang'), (2, 'bbbb')", "INSERT INTO `component` (`id`,`name`,`tenant`) VALUES (1,_UTF8MB4'zhang','gslq4dev'),(2,_UTF8MB4'bbbb','gslq4dev')"},
	{"pf", "insert into component (id, name, tenant) values(1, 'zhang', 'gslq'), (2, 'bbbb', 'gslq')", "INSERT INTO `component` (`id`,`name`,`tenant`) VALUES (1,_UTF8MB4'zhang',_UTF8MB4'gslq'),(2,_UTF8MB4'bbbb',_UTF8MB4'gslq')"},
	{"pf", "insert into global_ipm.component (id, name) values(1, 'zhang'), (2, 'bbbb')", "INSERT INTO `mock_global_ipm`.`component` (`id`,`name`,`tenant`) VALUES (1,_UTF8MB4'zhang','gslq4dev'),(2,_UTF8MB4'bbbb','gslq4dev')"},
	{"pf", "insert into mock_global_ipm.component (id, name) values(1, 'zhang'), (2, 'bbbb')", "INSERT INTO `mock_global_ipm`.`component` (`id`,`name`,`tenant`) VALUES (1,_UTF8MB4'zhang','gslq4dev'),(2,_UTF8MB4'bbbb','gslq4dev')"},
	{"dw", "insert into global_dw.component (id, name) values(1, 'zhang'), (2, 'bbbb')", "INSERT INTO `mock_global_dw_1`.`component` (`id`,`name`,`tenant`) VALUES (1,_UTF8MB4'zhang','gslq4dev'),(2,_UTF8MB4'bbbb','gslq4dev')"},
}

var insertSetCases = []mctechTestCase{
	{"pf", "insert into component set id = 1, name = 'zhang'", "INSERT INTO `component` SET `id`=1,`name`=_UTF8MB4'zhang',`tenant`='gslq4dev'"},
	{"pf", "insert into component set id = 1, name = 'zhang',tenant = 'gslq'", "INSERT INTO `component` SET `id`=1,`name`=_UTF8MB4'zhang',`tenant`=_UTF8MB4'gslq'"},
}

var insertOnDuplicateCases = []mctechTestCase{
	{"pf", "insert into component (id, name) values(1, 'zhang'), (2, 'bbbb') on duplicate key update name=values(name)", "INSERT INTO `component` (`id`,`name`,`tenant`) VALUES (1,_UTF8MB4'zhang','gslq4dev'),(2,_UTF8MB4'bbbb','gslq4dev') ON DUPLICATE KEY UPDATE `name`=VALUES(`name`)"},
	{"pf", "insert into component set id = 1, name = 'zhang' on duplicate key update name=values(name)", "INSERT INTO `component` SET `id`=1,`name`=_UTF8MB4'zhang',`tenant`='gslq4dev' ON DUPLICATE KEY UPDATE `name`=VALUES(`name`)"},
	{"pf", "insert into component (id, name) select id, name from component on duplicate key update name=values(name)", "INSERT INTO `component` (`id`,`name`,`tenant`) SELECT `id`,`name`,'gslq4dev' AS `tenant` FROM `component` WHERE (`component`.`tenant`='gslq4dev') ON DUPLICATE KEY UPDATE `name`=VALUES(`name`)"},
}

var selectFromJoinCases = []mctechTestCase{
	{"pf", "select a.id, b.name from component as a join component_param as b on a.id = b.component_id", "SELECT `a`.`id`,`b`.`name` FROM `component` AS `a` JOIN `component_param` AS `b` ON ((`b`.`tenant`='gslq4dev') AND (`a`.`id`=`b`.`component_id`)) WHERE (`a`.`tenant`='gslq4dev')"},
	{"pf", "select a.id, b.name from component as a inner join component_param as b on a.id = b.component_id left join component_image as c on a.id = c.component_id", "SELECT `a`.`id`,`b`.`name` FROM (`component` AS `a` JOIN `component_param` AS `b` ON ((`b`.`tenant`='gslq4dev') AND (`a`.`id`=`b`.`component_id`))) LEFT JOIN `component_image` AS `c` ON ((`c`.`tenant`='gslq4dev') AND (`a`.`id`=`c`.`component_id`)) WHERE (`a`.`tenant`='gslq4dev')"},
	{"pf", "select a.id, b.name from component as a inner join component_param as b on a.id = b.component_id left join component_image as c using (id, component_id)", "SELECT `a`.`id`,`b`.`name` FROM (`component` AS `a` JOIN `component_param` AS `b` ON ((`b`.`tenant`='gslq4dev') AND (`a`.`id`=`b`.`component_id`))) LEFT JOIN `component_image` AS `c` USING (`id`,`component_id`) WHERE ((`a`.`tenant`='gslq4dev') AND (`c`.`tenant`='gslq4dev'))"},
	{"pf", "select a.id, b.name from component as a, component_param as b where a.id = b.component_id", "SELECT `a`.`id`,`b`.`name` FROM (`component` AS `a`) JOIN `component_param` AS `b` WHERE (((`a`.`tenant`='gslq4dev') AND (`b`.`tenant`='gslq4dev')) AND (`a`.`id`=`b`.`component_id`))"},
	{"pf", "select a.id, b.name from component as a, component_param as b where a.id = b.component_id or a.id is null", "SELECT `a`.`id`,`b`.`name` FROM (`component` AS `a`) JOIN `component_param` AS `b` WHERE (((`a`.`tenant`='gslq4dev') AND (`b`.`tenant`='gslq4dev')) AND ((`a`.`id`=`b`.`component_id`) OR `a`.`id` IS NULL))"},
	{"pf", "select a.id, b.name, component_image.full_id from component as a, component_param as b, component_image where a.id = b.component_id and component_image.component_id = a.id", "SELECT `a`.`id`,`b`.`name`,`component_image`.`full_id` FROM ((`component` AS `a`) JOIN `component_param` AS `b`) JOIN `component_image` WHERE ((((`a`.`tenant`='gslq4dev') AND (`b`.`tenant`='gslq4dev')) AND (`component_image`.`tenant`='gslq4dev')) AND ((`a`.`id`=`b`.`component_id`) AND (`component_image`.`component_id`=`a`.`id`)))"},
}

var selectFromMultipleTasbleCases = []mctechTestCase{
	{"pf", "select a.id, b.name from component as a, component_param as b where a.id = b.component_id", "SELECT `a`.`id`,`b`.`name` FROM (`component` AS `a`) JOIN `component_param` AS `b` WHERE (((`a`.`tenant`='gslq4dev') AND (`b`.`tenant`='gslq4dev')) AND (`a`.`id`=`b`.`component_id`))"},
	{"pf", "select a.id, b.name, component_image.full_id from component as a, component_param as b, component_image where a.id = b.component_id and component_image.component_id = a.id", "SELECT `a`.`id`,`b`.`name`,`component_image`.`full_id` FROM ((`component` AS `a`) JOIN `component_param` AS `b`) JOIN `component_image` WHERE ((((`a`.`tenant`='gslq4dev') AND (`b`.`tenant`='gslq4dev')) AND (`component_image`.`tenant`='gslq4dev')) AND ((`a`.`id`=`b`.`component_id`) AND (`component_image`.`component_id`=`a`.`id`)))"},
}

var selectFromSubqueryCases = []mctechTestCase{
	{"pf", "select * from component_param as b where b.component_id in (select id from component as a where a.id > 100)", "SELECT * FROM `component_param` AS `b` WHERE ((`b`.`tenant`='gslq4dev') AND `b`.`component_id` IN (SELECT `id` FROM `component` AS `a` WHERE ((`a`.`tenant`='gslq4dev') AND (`a`.`id`>100))))"},
	{"pf", "select * from (select * from component_param where component_id > 100) a where is_removed = false", "SELECT * FROM (SELECT * FROM `component_param` WHERE ((`component_param`.`tenant`='gslq4dev') AND (`component_id`>100))) AS `a` WHERE (`is_removed`=FALSE)"},
}

var selectWithCTECases = []mctechTestCase{
	{"pf", "with tmp as (select * from component_param as b where b.component_id is not null) select * from Tmp", "WITH `tmp` AS (SELECT * FROM `component_param` AS `b` WHERE ((`b`.`tenant`='gslq4dev') AND `b`.`component_id` IS NOT NULL)) SELECT * FROM `Tmp`"},
	{"pf", "with tmp1 as (select * from component_param where component_id is not null ),tmp2 as (select * from component_image where component_id is not null) select tmp1.* from tmp1 inner join tmp2 on tmp1.image_id = tmp2.id", "WITH `tmp1` AS (SELECT * FROM `component_param` WHERE ((`component_param`.`tenant`='gslq4dev') AND `component_id` IS NOT NULL)), `tmp2` AS (SELECT * FROM `component_image` WHERE ((`component_image`.`tenant`='gslq4dev') AND `component_id` IS NOT NULL)) SELECT `tmp1`.* FROM `tmp1` JOIN `tmp2` ON (`tmp1`.`image_id`=`tmp2`.`id`)"},
}

var selectFunctionCases = []mctechTestCase{
	{"pf", "select sum(stat_year) / 10000 as month_amount from project_record", "SELECT (SUM(`stat_year`)/10000) AS `month_amount` FROM `project_record` WHERE (`project_record`.`tenant`='gslq4dev')"},
}

var selectStarCases = []mctechTestCase{
	{"pf", "insert into entry_work (name, id) select a.*, b.id from component as a join component_param as b on a.id = b.component_id", "INSERT INTO `entry_work` (`name`,`id`,`tenant`) SELECT `a`.*,`b`.`id`,'gslq4dev' AS `tenant` FROM `component` AS `a` JOIN `component_param` AS `b` ON ((`b`.`tenant`='gslq4dev') AND (`a`.`id`=`b`.`component_id`)) WHERE (`a`.`tenant`='gslq4dev')"},
}

var selectUnionCases = []mctechTestCase{
	{"pf", "select id, name from component_param as a where a.id > 100 union select id, name from component_param as b where b.name = ''", "SELECT `id`,`name` FROM `component_param` AS `a` WHERE ((`a`.`tenant`='gslq4dev') AND (`a`.`id`>100)) UNION SELECT `id`,`name` FROM `component_param` AS `b` WHERE ((`b`.`tenant`='gslq4dev') AND (`b`.`name`=_UTF8MB4''))"},
}

var simpleCases = []mctechTestCase{
	{"pf", "select 1", "SELECT 1"},
	{"pf", "select 1 as tenant", "SELECT 1 AS `tenant`"},
	{"pf", "select * from component", "SELECT * FROM `component` WHERE (`component`.`tenant`='gslq4dev')"},
	{"pf", "select id, name from component", "SELECT `id`,`name` FROM `component` WHERE (`component`.`tenant`='gslq4dev')"},
	{"pf", "select `select`, name from component", "SELECT `select`,`name` FROM `component` WHERE (`component`.`tenant`='gslq4dev')"},
	{"pf", "select `select`, name from component where id > 1000", "SELECT `select`,`name` FROM `component` WHERE ((`component`.`tenant`='gslq4dev') AND (`id`>1000))"},
	{"pf", "select `select`, name from component where id > 1000 or id is null", "SELECT `select`,`name` FROM `component` WHERE ((`component`.`tenant`='gslq4dev') AND ((`id`>1000) OR `id` IS NULL))"},
}

var updateSingleTableCases = []mctechTestCase{
	{"pf", "update component set name = 'bbb' where id > 100", "UPDATE `component` SET `name`=_UTF8MB4'bbb' WHERE ((`component`.`tenant`='gslq4dev') AND (`id`>100))"},
	{"pf", "update component a join component_param as b on a.id = b.component_id set name = 'bbb'", "UPDATE `component` AS `a` JOIN `component_param` AS `b` ON ((`b`.`tenant`='gslq4dev') AND (`a`.`id`=`b`.`component_id`)) SET `name`=_UTF8MB4'bbb' WHERE (`a`.`tenant`='gslq4dev')"},
}

var updateMultipleTableCases = []mctechTestCase{
	{"pf", "update items, month set items.price=month.price where items.id=month.id", "UPDATE (`items`) JOIN `month` SET `items`.`price`=`month`.`price` WHERE (((`items`.`tenant`='gslq4dev') AND (`month`.`tenant`='gslq4dev')) AND (`items`.`id`=`month`.`id`))"},
}

var updateWithSubqueryCases = []mctechTestCase{
	{"pf", "update items set retail = retail * 0.9 where id in (select id from items where retail / wholesale >= 1.3 and quantity > 100)", "UPDATE `items` SET `retail`=(`retail`*0.9) WHERE ((`items`.`tenant`='gslq4dev') AND `id` IN (SELECT `id` FROM `items` WHERE ((`items`.`tenant`='gslq4dev') AND (((`retail`/`wholesale`)>=1.3) AND (`quantity`>100)))))"},
	{"pf", "update items, (select id from items where id in (select id from items where retail / wholesale >= 1.3 and quantity < 100)) as discounted set items.retail = items.retail * 0.9 where items.id = discounted.id", "UPDATE (`items`) JOIN (SELECT `id` FROM `items` WHERE ((`items`.`tenant`='gslq4dev') AND `id` IN (SELECT `id` FROM `items` WHERE ((`items`.`tenant`='gslq4dev') AND (((`retail`/`wholesale`)>=1.3) AND (`quantity`<100)))))) AS `discounted` SET `items`.`retail`=(`items`.`retail`*0.9) WHERE ((`items`.`tenant`='gslq4dev') AND (`items`.`id`=`discounted`.`id`))"},
}

var updateWithCTECases = []mctechTestCase{
	{"pf", "with tmp as (select * from component_param as b where b.component_id is not null) update component set version=mctech_sequence() where id in (select id from Tmp)", "WITH `tmp` AS (SELECT * FROM `component_param` AS `b` WHERE ((`b`.`tenant`='gslq4dev') AND `b`.`component_id` IS NOT NULL)) UPDATE `component` SET `version`=MCTECH_SEQUENCE() WHERE ((`component`.`tenant`='gslq4dev') AND `id` IN (SELECT `id` FROM `Tmp`))"},
	{"pf", "with tmp as (select * from component_param as b where b.component_id is not null) update component a join Tmp b on a.id = b.id set version=mctech_sequence() ", "WITH `tmp` AS (SELECT * FROM `component_param` AS `b` WHERE ((`b`.`tenant`='gslq4dev') AND `b`.`component_id` IS NOT NULL)) UPDATE `component` AS `a` JOIN `Tmp` AS `b` ON (`a`.`id`=`b`.`id`) SET `version`=MCTECH_SEQUENCE() WHERE (`a`.`tenant`='gslq4dev')"},
}

var cases = [][]mctechTestCase{
	deleteSingleTableCases, deleteMultipleTableCases, deleteWithCTECases,
	insertIntoSelectCases, insertIntoValuesCases, insertSetCases, insertOnDuplicateCases,
	selectFromJoinCases, selectFromMultipleTasbleCases, selectFromSubqueryCases, selectWithCTECases, selectFunctionCases, selectStarCases, selectUnionCases,
	simpleCases,
	updateSingleTableCases, updateMultipleTableCases, updateWithSubqueryCases, updateWithCTECases,
}

func TestTenantVisitor(t *testing.T) {
	for _, lst := range cases {
		doRunTest(t, lst, true)
	}
}
