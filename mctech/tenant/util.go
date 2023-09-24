package tenant

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
)

func ApplyTenantIsolation(context mctech.MCTechContext, node ast.Node,
	charset string, collation string) (dbs []string, skipped bool) {
	switch node.(type) {
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt, *ast.InsertStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt,
		*ast.ExplainStmt:
		v := NewTenantVisitor(context, charset, collation)
		node.Accept(v)

		dbs = make([]string, len(v.dbNames)+1)
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
		return dbs, false
	}

	return nil, true
}
