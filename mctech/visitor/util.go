package visitor

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
)

// ApplyExtension apply tenant condition
func ApplyExtension(mctx mctech.Context, node ast.Node,
	charset, collation string) (dbs []string, skipped bool, err error) {
	skipped = false
	switch stmtNode := node.(type) {
	case *ast.SelectStmt:
		dbs, err = doApplyExtension(mctx, stmtNode, charset, collation)
		if stmtNode.Kind == ast.SelectStmtKindTable {
			// "desc global_xxx.table" 语句解析后生成的SelectStmt
			skipped = true
		}
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.InsertStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt,
		*ast.LoadDataStmt,
		*ast.NonTransactionalDMLStmt, // BATCH ......
		*ast.TruncateTableStmt:
		dbs, err = doApplyExtension(mctx, stmtNode, charset, collation)
	case *ast.MCTechStmt:
		// MCTechStmt只需要处理对应的子句就可以
		dbs, skipped, err = ApplyExtension(mctx, stmtNode.Stmt, charset, collation)
	case *ast.ExplainStmt:
		// ExplainStmt只需要处理对应的子句就可以
		dbs, skipped, err = ApplyExtension(mctx, stmtNode.Stmt, charset, collation)
	default:
		skipped = true
	}

	return dbs, skipped, err
}

func doApplyExtension(
	mctx mctech.Context, node ast.Node, charset, collation string) (dbs []string, err error) {
	var v dbNameVisitor
	if mctx.InExecute() {
		v = newDatabaseNameVisitor(mctx)
	} else {
		v = newIsolationConditionVisitor(mctx, charset, collation)
	}
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	node.Accept(v)

	dbs = make([]string, 0, len(v.DBNames()))
	for n := range v.DBNames() {
		if n == "" {
			n = mctx.CurrentDB()
		}
		dbs = append(dbs, n)
	}
	return dbs, err
}
