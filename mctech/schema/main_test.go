package schema

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/pingcap/tidb/testkit"
)

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_platform")
	tk.MustExec("create database global_platform")
	tk.MustExec("use global_platform")
	tk.MustExec("create table t(a int, b int, key(b))")
	s := tk.Session()
	s.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}
	return tk
}
