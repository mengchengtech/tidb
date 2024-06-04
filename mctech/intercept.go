package mctech

import (
	"errors"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/intest"
)

// Interceptor mctech enhance interface
type Interceptor interface {
	// BeforeParseSQL
	BeforeParseSQL(sctx sessionctx.Context, sql string) (Context, string, error)
	// AfterParseSQL
	AfterParseSQL(sctx sessionctx.Context, stmt ast.StmtNode) (err error)
	// ParseSQLFailed
	ParseSQLFailed(sctx sessionctx.Context, sql string, err error)
	// AfterHandleStmt
	AfterHandleStmt(sctx sessionctx.Context, stmt ast.StmtNode, err error)
}

var interceptor Interceptor

// SetInterceptor function
func SetInterceptor(i Interceptor) {
	interceptor = i
}

// GetInterceptor function
func GetInterceptor() Interceptor {
	if interceptor == nil && !intest.InTest {
		err := errors.New("function variable 'mctech.GetInterceptor' is nil")
		panic(err)
	}
	return interceptor
}
