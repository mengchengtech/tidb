// add by zhangbing

package server

import (
	"context"

	"github.com/pingcap/tidb/mctech"
	_ "github.com/pingcap/tidb/mctech/preps" // 强制初始化preps
	"github.com/pingcap/tidb/parser/ast"
)

func (cc *clientConn) onBeforeParseSQL(ctx context.Context, sql string) (context.Context, mctech.Context, string, error) {
	return mctech.GetInterceptor().BeforeParseSQL(ctx, cc.getCtx(), sql)
}

func (cc *clientConn) onAfterParseSQL(ctx context.Context, mctx mctech.Context, stmts []ast.StmtNode) (err error) {
	it := mctech.GetInterceptor()
	sctx := cc.getCtx()
	for _, stmt := range stmts {
		if err = it.AfterParseSQL(ctx, sctx, mctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (cc *clientConn) onAfterHandleStmt(ctx context.Context, stmt ast.StmtNode, err error) {
	mctech.GetInterceptor().AfterHandleStmt(ctx, cc.getCtx(), stmt, err)
}
