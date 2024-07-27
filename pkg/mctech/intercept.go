package mctech

import (
	"context"
	"errors"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Interceptor mctech enhance interface
type Interceptor interface {
	// BeforeParseSQL
	BeforeParseSQL(ctx context.Context, sess sessionctx.Context, sql string) (context.Context, Context, string, error)
	// AfterParseSQL
	AfterParseSQL(ctx context.Context, sess sessionctx.Context, mctx Context, stmt ast.StmtNode) (err error)
	// AfterHandleStmt
	AfterHandleStmt(ctx context.Context, sess sessionctx.Context, stmt ast.StmtNode, err error)
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
