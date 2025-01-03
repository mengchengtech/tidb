package mctech

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// Handler mctech enhance interface
type Handler interface {
	// PrapareSQL prapare sql
	PrapareSQL(session sessionctx.Context, rawSql string) (sql string, err error)
	// ApplyAndCheck apply tenant isolation and check db policies
	ApplyAndCheck(session sessionctx.Context, stmts []ast.StmtNode) (changed bool, err error)
}

// HandlerFactory create Handler instance
type HandlerFactory interface {
	CreateHandler() Handler
	CreateHandlerWithContext(ctx Context) Handler
}

const handlerFactoryKey sessionValueKey = "$$MCTechContext"

// GetHandlerFactory get HandlerFactory from session
func GetHandlerFactory(s sessionctx.Context) HandlerFactory {
	if factory, ok := s.Value(handlerFactoryKey).(HandlerFactory); ok {
		return factory
	}
	return nil
}

// SetHandlerFactory set HandlerFactory to session
func SetHandlerFactory(s sessionctx.Context, factory HandlerFactory) {
	s.SetValue(handlerFactoryKey, factory)
}
