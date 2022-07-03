package tenant

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
)

func ApplyTenantIsolation(context mctech.MCTechContext, node ast.Node,
	charset string, collation string) (dbs []string, skipped bool, err error) {
	switch node.(type) {
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt, *ast.InsertStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt,
		*ast.ExplainStmt:
		v := NewTenantVisitor(context, charset, collation)
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
		return dbs, false, nil
	}

	return nil, true, nil
}
