package tenant

import (
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

func ApplyTenantIsolation(ctx sessionctx.Context, node ast.Node) {
	switch node.(type) {
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt, *ast.InsertStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt,
		*ast.ExplainStmt:
		vars := ctx.GetSessionVars()
		charset, collation := vars.GetCharsetInfo()
		v := NewTenantVisitor("", 1, false, nil, vars.CurrentDB, "gslq", charset, collation)
		node.Accept(v)

		// var sb strings.Builder
		// node.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreBracketAroundBinaryOperation, &sb))
		// restoreSQL := sb.String()
		// log.Info("<" + vars.CurrentDB + ">" + restoreSQL + "\r\n --> " + string(debug.Stack()))
	}
}
