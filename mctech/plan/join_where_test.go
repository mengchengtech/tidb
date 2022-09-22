package plan

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
)

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test")
	tk.MustExec("create database test")
	tk.MustExec("use test")
	s := tk.Session()
	s.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}
	tk.MustExec(strings.Join([]string{
		"CREATE TABLE `unit_test` (",
		"`id` bigint(20) NOT NULL,",
		"`value` varchar(50) NOT NULL,",
		"`decimal_field` DECIMAL(10,3) NOT NULL DEFAULT '0',",
		"`datetime_field` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,",
		" PRIMARY KEY (`id`)",
		")",
	}, "\n"),
	)
	return tk
}

func TestJoinAndWhere(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)
	tk.MustExec(strings.Join([]string{
		"SELECT * FROM unit_test",
		"  INNER JOIN (SELECT 1 id, 'a' `value`) a ON unit_test.id=a.id AND unit_test.value=a.value",
		"WHERE unit_test.id IN (1) AND unit_test.value IN ('a')",
	}, "\n"))
}
