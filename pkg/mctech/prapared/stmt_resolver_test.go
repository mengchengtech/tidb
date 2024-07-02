package prapared

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/mctech/tenant"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	. "github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestStmtResolver(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_platform")
	tk.MustExec("create database global_platform")
	tk.MustExec("use global_platform")
	tk.MustExec("create table t(a int, b int, key(b))")
	session := tk.Session()
	session.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	sql := "/*& global:!ys2 */ select * from company"
	resolver := NewStatementResolver()
	_, err := resolver.PrepareSql(session, sql)
	require.NoError(t, err)
	mctx := resolver.Context()

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	require.NoErrorf(t, err, "source %v", sql)
	comment := fmt.Sprintf("source %v", sql)
	stmt := stmts[0]
	var sb strings.Builder
	visitor := tenant.NewTenantVisitor(mctx, "", "")
	stmt.Accept(visitor)
	err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreBracketAroundBinaryOperation, &sb))
	require.NoError(t, err, comment)
	restoreSQL := sb.String()

	expect := "SELECT * FROM `company` WHERE (`company`.`tenant`='gdcd')"
	require.Equalf(t, expect, restoreSQL, "restore %v; expect %v", restoreSQL, expect)

}
