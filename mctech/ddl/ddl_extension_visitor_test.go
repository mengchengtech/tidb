package ddl

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser"
	. "github.com/pingcap/tidb/parser/format"
	"github.com/stretchr/testify/require"
)

type ddlMCTechTestCase struct {
	shortDb string
	sql     string
	expect  string
	failure string
}

func (t ddlMCTechTestCase) Source() any {
	return t.sql
}

func (t *ddlMCTechTestCase) Expect() string {
	return t.expect
}

func (t *ddlMCTechTestCase) Failure() string {
	return t.failure
}

var ErrColumnDef = "'__version' 字段定义不正确，允许的定义为 -> __version BIGINT NOT NULL DEFAULT MCTECH_SEQUENCE() ON UPDATE MCTECH_SEQUENCE()"
var (
	nopCases = []*ddlMCTechTestCase{
		{"test", "create table t0 (id bigint,name varchar(100),primary key (id))", "CREATE TABLE `t0` (`id` BIGINT,`name` VARCHAR(100),PRIMARY KEY(`id`))", ""},
		{"test", "alter table t1 add column (__version bigint)", "ALTER TABLE `t1` ADD COLUMN (`__version` BIGINT)", ""},
	}
	createCases = []*ddlMCTechTestCase{
		{"pf", "create table t0 (id bigint,name varchar(100),primary key (id))", "CREATE TABLE `t0` (`id` BIGINT,`name` VARCHAR(100),`__version` BIGINT NOT NULL DEFAULT `MCTECH_SEQUENCE`() ON UPDATE `MCTECH_SEQUENCE`(),PRIMARY KEY(`id`))", ""},
	}
	alterAddMultiColumnsCases = []*ddlMCTechTestCase{
		// add multiple
		{"pf", "alter table t1 add column (name varchar(100),__version bigint, age int not null)", "", ErrColumnDef},
		{"pf", "alter table t1 add column (__version bigint)", "", ErrColumnDef},
		{"pf", "alter table t1 add column (__version bigint not null)", "", ErrColumnDef},
		{"pf", "alter table t1 add column (__version bigint not null default mctech_sequence())", "", ErrColumnDef},
		{"pf", "alter table t1 add column (__version bigint not null default mctech_sequence() on update mctech_sequence())", "ALTER TABLE `t1` ADD COLUMN (`__version` BIGINT NOT NULL DEFAULT MCTECH_SEQUENCE() ON UPDATE MCTECH_SEQUENCE())", ""},
		{"pf", "alter table t1 add column (__version bigint not null default mctech_sequence() on update mctech_sequence() comment 'abc')", "ALTER TABLE `t1` ADD COLUMN (`__version` BIGINT NOT NULL DEFAULT MCTECH_SEQUENCE() ON UPDATE MCTECH_SEQUENCE() COMMENT 'abc')", ""},
	}
	alterAddSingleColumnCases = []*ddlMCTechTestCase{
		// add single
		{"pf", "alter table t2 add column __version bigint", "", ErrColumnDef},
		{"pf", "alter table t2 add column __version bigint not null default mctech_sequence()", "", ErrColumnDef},
		{"pf", "alter table t2 add column __version bigint not null on update mctech_sequence() default mctech_sequence()", "ALTER TABLE `t2` ADD COLUMN `__version` BIGINT NOT NULL ON UPDATE MCTECH_SEQUENCE() DEFAULT MCTECH_SEQUENCE()", ""},
		{"pf", "alter table t2 add column __version bigint not null on update mctech_sequence() default mctech_sequence() comment 'abc'", "ALTER TABLE `t2` ADD COLUMN `__version` BIGINT NOT NULL ON UPDATE MCTECH_SEQUENCE() DEFAULT MCTECH_SEQUENCE() COMMENT 'abc'", ""},
	}

	alterRenameCases = []*ddlMCTechTestCase{
		// rename
		{"pf", "alter table t3 rename column __version to xxxx_field", "", "'__version' 字段不支持修改名称"},
		{"pf", "alter table t3 rename column xxxx_field to __version", "", "不支持把其它字段名称修改为'__version'"},
		{"pf", "alter table t3 rename column field1 to field2", "ALTER TABLE `t3` RENAME COLUMN `field1` TO `field2`", ""},
	}
	alterDropCases = []*ddlMCTechTestCase{
		// drop
		{"pf", "alter table t4 drop column __version", "", "'__version' 字段不允许删除"},
		{"pf", "alter table t4 drop column field1", "ALTER TABLE `t4` DROP COLUMN `field1`", ""},
	}

	alterChangeCases = []*ddlMCTechTestCase{
		// change
		{"pf", "alter table t5 change column __version __version bigint", "", ErrColumnDef},
		{"pf", "alter table t5 change column __version field1 bigint not null default mctech_sequence()", "", "'__version' 字段不支持修改名称"},
		{"pf", "alter table t5 change column field1 __version bigint not null default mctech_sequence()", "", "不支持把其它字段名称修改为'__version'"},
		{"pf", "alter table t5 change column __version __version bigint not null default mctech_sequence on update mctech_sequence", "ALTER TABLE `t5` CHANGE COLUMN `__version` `__version` BIGINT NOT NULL DEFAULT MCTECH_SEQUENCE() ON UPDATE MCTECH_SEQUENCE()", ""},
		{"pf", "alter table t5 change column __version __version bigint not null default mctech_sequence", "", ErrColumnDef},
		{"pf", "alter table t5 change column __version __version bigint not null on update mctech_sequence() default mctech_sequence()", "ALTER TABLE `t5` CHANGE COLUMN `__version` `__version` BIGINT NOT NULL ON UPDATE MCTECH_SEQUENCE() DEFAULT MCTECH_SEQUENCE()", ""},
		{"pf", "alter table t5 change column field1 field1 bigint", "ALTER TABLE `t5` CHANGE COLUMN `field1` `field1` BIGINT", ""},
	}

	alterAlterCases = []*ddlMCTechTestCase{
		// alter default
		{"pf", "alter table t6 alter column __version drop default", "", "'__version' 字段不允删除默认值"},
		{"pf", "alter table t6 alter column __version set default (now())", "", "'__version' 字段不允修改默认值"},
		{"pf", "alter table t6 alter column field set default (current_timestamp)", "ALTER TABLE `t6` ALTER COLUMN `field` SET DEFAULT (CURRENT_TIMESTAMP())", ""},
		{"pf", "alter table t6 alter column field set default (current_timestamp())", "ALTER TABLE `t6` ALTER COLUMN `field` SET DEFAULT (CURRENT_TIMESTAMP())", ""},
		{"pf", "alter table t6 alter column __version set default (mctech_sequence())", "ALTER TABLE `t6` ALTER COLUMN `__version` SET DEFAULT (MCTECH_SEQUENCE())", ""},
		{"pf", "alter table t6 alter column __version set default (mctech_sequence)", "ALTER TABLE `t6` ALTER COLUMN `__version` SET DEFAULT (MCTECH_SEQUENCE())", ""},
	}
)

var ddlExtensionCases = [][]*ddlMCTechTestCase{
	nopCases,
	createCases, alterAddMultiColumnsCases, alterAddSingleColumnCases,
	alterRenameCases, alterDropCases, alterChangeCases, alterAlterCases,
}

func TestDDLExtensionVisitor(t *testing.T) {
	for _, lst := range ddlExtensionCases {
		doRunTest(t, doRunDDLMCTechTestCase, lst)
	}
}

func doRunDDLMCTechTestCase(t *testing.T, c *ddlMCTechTestCase) error {
	p := parser.New()
	stmts, _, err := p.Parse(c.sql, "", "")
	require.NoErrorf(t, err, "source %v", c.sql)
	var sb strings.Builder
	restoreSQLs := ""
	db, ok := dbMap[c.shortDb]
	if !ok {
		db = c.shortDb
	}
	for _, stmt := range stmts {
		sb.Reset()
		if err := ApplyExtension(db, stmt); err != nil {
			return err
		}
		err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreBracketAroundBinaryOperation, &sb))
		if err != nil {
			return err
		}

		restoreSQL := sb.String()
		if restoreSQLs != "" {
			restoreSQLs += "; "
		}
		restoreSQLs += restoreSQL

	}
	require.Equalf(t, c.expect, restoreSQLs, "restore %v; expect %v", restoreSQLs, c.expect)
	return nil
}
