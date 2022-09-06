package mctech

import (
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

// Handler mctech enhance interface
type Handler interface {
	// PrepareSQL prapare sql
	PrepareSQL(session sessionctx.Context, rawSql string) (sql string, err error)
	// ApplyAndCheck apply tenant isolation and check db policies
	ApplyAndCheck(session sessionctx.Context, stmts []ast.StmtNode) (changed bool, err error)
}

// HandlerFactory create Handler instance
type HandlerFactory interface {
	CreateHandler() Handler
	CreateHandlerWithContext(ctx Context) Handler
}

const handlerFactoryKey sessionValueKey = "$$MCTechContext"

// GetHandlerFactoryForTest get HandlerFactory from session
func GetHandlerFactoryForTest(s sessionctx.Context) HandlerFactory {
	if factory, ok := s.Value(handlerFactoryKey).(HandlerFactory); ok {
		return factory
	}
	return nil
}

// SetHandlerFactoryForTest set HandlerFactory to session
func SetHandlerFactoryForTest(s sessionctx.Context, factory HandlerFactory) {
	s.SetValue(handlerFactoryKey, factory)
}
