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
