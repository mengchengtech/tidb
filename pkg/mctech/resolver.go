package mctech

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

type StatementResolver interface {
	Context() MCTechContext
	PrepareSql(ctx sessionctx.Context, sql string) (string, error)
	ResolveStmt(stmt ast.Node, charset string, collation string) error
	Validate(ctx sessionctx.Context) error
}
