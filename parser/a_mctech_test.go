package parser_test

import (
	"testing"
)

func TestMCTechFunctionDelete(t *testing.T) {
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
	}

	RunTest(t, cases, false)
}
