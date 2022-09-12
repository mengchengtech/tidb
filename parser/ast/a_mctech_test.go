package ast_test

import (
	"testing"

	"github.com/pingcap/tidb/parser/ast"
)

func TestMCTechStmtVisitorCover(t *testing.T) {
	stmts := []ast.Node{
		&ast.MCTechStmt{Stmt: &ast.ShowStmt{}},
	}

	for _, v := range stmts {
		v.Accept(visitor{})
		v.Accept(visitor1{})
	}
}

func TestMCTechFuncCallExprRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"mctech_sequence()", "MCTECH_SEQUENCE()"},
	}
	extractNodeFunc := func(node ast.Node) ast.Node {
		return node.(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	runNodeRestoreTest(t, testCases, "select %s", extractNodeFunc)
}
