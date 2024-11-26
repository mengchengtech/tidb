package testkit

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/mctech"
	_ "github.com/pingcap/tidb/pkg/mctech/preps" // 强制调用preps包里的init方法
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (tk *TestKit) onBeforeParseSQL(ctx context.Context, sql string) (context.Context, mctech.Context, string, error) {
	handler := mctech.GetHandler()
	subCtx, mctx, err := mctech.WithNewContext3(ctx, tk.Session(), false)
	if err != nil {
		return subCtx, nil, "", err
	}

	if mctx != nil {
		if sql, err = handler.PrepareSQL(mctx, sql); err != nil {
			return ctx, nil, "", err
		}
	}
	return subCtx, mctx, sql, nil
}

func (tk *TestKit) onAfterParseSQL(ctx context.Context, mctx mctech.Context, sql string, stmts []ast.StmtNode) (err error) {
	if mctx != nil {
		handler := mctech.GetHandler()
		for _, stmt := range stmts {
			if _, err = handler.ApplyAndCheck(mctx, stmt); err != nil {
				if strFmt, ok := tk.session.(fmt.Stringer); ok {
					logutil.Logger(ctx).Warn("mctech SQL failed", zap.Error(err), zap.Stringer("session", strFmt), zap.String("SQL", sql))
				}
				return err
			}
		}
	}
	return nil
}
