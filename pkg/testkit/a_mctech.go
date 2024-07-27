package testkit

import (
	"context"

	"github.com/pingcap/tidb/pkg/mctech"
	_ "github.com/pingcap/tidb/pkg/mctech/preps" // 强制调用preps包里的init方法
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func (tk *TestKit) onBeforeParseSQL(ctx context.Context, sql string) (context.Context, mctech.Context, string, error) {
	return mctech.GetInterceptor().BeforeParseSQL(ctx, tk.Session(), sql)
}

func (tk *TestKit) onAfterParseSQL(ctx context.Context, mctx mctech.Context, stmts []ast.StmtNode) (err error) {
	it := mctech.GetInterceptor()
	sctx := tk.Session()
	for _, stmt := range stmts {
		if err = it.AfterParseSQL(ctx, sctx, mctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (tk *TestKit) onAfterHandleStmt(ctx context.Context, stmt ast.StmtNode, err error) {
	mctech.GetInterceptor().AfterHandleStmt(ctx, tk.Session(), stmt, err)
}
