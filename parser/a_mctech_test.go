// add by zhangbing

package parser_test

import (
	"testing"
)

func TestMCTechFunction(t *testing.T) {
	cases := []testCase{
		{"/*& tenant:gslq */ select mctech_sequence() as full_id", true,
			"SELECT MCTECH_SEQUENCE() AS `full_id`"},
	}

	RunTest(t, cases, false)
}

func TestMCTechStmt(t *testing.T) {
	cases := []testCase{
		{"mctech SELECT * FROM test.demo", true, "MCTECH SELECT * FROM `test`.`demo`"},
		{"mctech FORMAT = row SELECT * FROM test.demo", true, "MCTECH SELECT * FROM `test`.`demo`"},
		{"mctech FORMAT = 'row' SELECT * FROM test.demo", true, "MCTECH SELECT * FROM `test`.`demo`"},
		{"mctech format='json' select * from user", true, "MCTECH FORMAT = 'json' SELECT * FROM `user`"},
		{"mctech seq_decode 1310341421945856", true, "MCTECH SEQ_DECODE 1310341421945856"},
		{"mctech select * from user", true, "MCTECH SELECT * FROM `user`"},
		{"mctech show help", true, "MCTECH SHOW HELP"},
		{"mctech show help true", true, "MCTECH SHOW HELP TRUE"},
		{"mctech show dw_index", true, "MCTECH SHOW DW_INDEX"},
		{"mctech show deny_digest", true, "MCTECH SHOW DENY_DIGEST"},
		{"mctech show full_sql '2026-03-01 12:00:01.123' 123456789 123456789", true, "MCTECH SHOW FULL_SQL '2026-03-01 12:00:01.123' 123456789 123456789"},
		{"mctech show full_sql '2026-03-01 12:00:01.123' 123456789 123456789 'product'", true, "MCTECH SHOW FULL_SQL '2026-03-01 12:00:01.123' 123456789 123456789 'product'"},
	}

	RunTest(t, cases, false)
}
