package mctech

import (
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

// Handler mctech enhance interface
type Handler interface {
	// PrapareSQL prapare sql
	PrapareSQL() (sql string, err error)
	// ApplyAndCheck apply tenant isolation and check db policies
	ApplyAndCheck(stmts []ast.StmtNode) (changed bool, err error)
}

// HandlerFactory create Handler instance
type HandlerFactory interface {
	CreateHandler(session sessionctx.Context, sql string) Handler
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
