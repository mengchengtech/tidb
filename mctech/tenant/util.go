package tenant

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
)

// ApplyTenantIsolation apply tenant condition
func ApplyTenantIsolation(context mctech.Context, node ast.Node,
	charset, collation string) (dbs []string, skipped bool, err error) {

	var stmtNode ast.Node
	if explainStmt, ok := node.(*ast.ExplainStmt); ok {
		// ExplainStmt只需要处理对应的子句就可以
		stmtNode = explainStmt.Stmt
	} else {
		stmtNode = node
	}

	switch stmtNode.(type) {
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt, *ast.InsertStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt:
		dbs, err = doApplyTenantIsolation(context, node, charset, collation)
		return dbs, false, err
	}

	return nil, true, nil
}

func doApplyTenantIsolation(
	context mctech.Context, node ast.Node, charset, collation string) (dbs []string, err error) {
	v := newVisitor(context, charset, collation)
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
	// var sb strings.Builder
	// node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreBracketAroundBinaryOperation, &sb))
	// restoreSQL := sb.String()
	// log.Info("<" + vars.CurrentDB + ">" + restoreSQL + "\r\n --> " + string(debug.Stack()))
	return dbs, err
}
