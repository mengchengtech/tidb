package mctech

import (
	"github.com/pingcap/tidb/parser/ast"
)

// Handler mctech enhance interface
type Handler interface {
	// PrepareSQL
	PrepareSQL(ctx Context, rawSQL string) (sql string, err error)
	// ApplyAndCheck
	ApplyAndCheck(ctx Context, stmts []ast.StmtNode) (changed bool, err error)
}

var handler Handler

// SetHandler function
func SetHandler(h Handler) {
	handler = h
}

// GetHandler function
func GetHandler() Handler {
	return handler
}
