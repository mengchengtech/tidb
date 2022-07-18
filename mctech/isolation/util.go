package isolation

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
)

// ApplyExtension apply tenant condition
func ApplyExtension(context mctech.Context, node ast.Node,
	charset, collation string) (dbs []string, skipped bool, err error) {
	skipped = false
	switch stmtNode := node.(type) {
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt, *ast.InsertStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt:
		dbs, err = doApplyExtension(context, stmtNode, charset, collation)
	case *ast.MCTechStmt:
		// MCTechStmt只需要处理对应的子句就可以
		dbs, err = doApplyExtension(context, stmtNode.Stmt, charset, collation)
	case *ast.ExplainStmt:
		// ExplainStmt只需要处理对应的子句就可以
		dbs, err = doApplyExtension(context, stmtNode.Stmt, charset, collation)
	default:
		skipped = true
	}

	return dbs, false, err
}

func doApplyExtension(
	context mctech.Context, node ast.Node, charset, collation string) (dbs []string, err error) {
	v := newIsolationConditionVisitor(context, charset, collation)
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	node.Accept(v)

	dbs = make([]string, 0, len(v.dbNames))
	for n := range v.dbNames {
		if n == "" {
			n = context.CurrentDB()
		}
		dbs = append(dbs, n)
	}
	return dbs, err
}
