package server

import (
	// 强制初始化preps
	"context"
	"fmt"

	"github.com/pingcap/tidb/mctech"
	_ "github.com/pingcap/tidb/mctech/preps"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func (cc *clientConn) beforeParseSql(ctx context.Context, sql string) (context.Context, mctech.Context, string, error) {
	if strFmt, ok := cc.getCtx().Session.(fmt.Stringer); ok {
		logutil.Logger(ctx).Warn("mctech SQL handleQuery", zap.Stringer("session", strFmt), zap.String("SQL", sql))
	}

	handler := mctech.GetHandler()
	subCtx, mctx, err := mctech.WithNewContext3(ctx, cc.ctx.Session, false)
	if err != nil {
		return subCtx, nil, "", err
	}

	if mctx != nil {
		if sql, err = handler.PrepareSQL(mctx, sql); err != nil {
			return subCtx, nil, "", err
		}
	}

	return subCtx, mctx, sql, nil
}

func (cc *clientConn) afterParseSql(ctx context.Context, mctx mctech.Context, sql string, stmts []ast.StmtNode) (err error) {
	// 判断当前是否是查询语句
	queryOnly := false
	for _, stmt := range stmts {
		switch stmtNode := stmt.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt:
			queryOnly = true
		case *ast.MCTechStmt:
			_, queryOnly = stmtNode.Stmt.(*ast.SelectStmt)
		case *ast.ExplainStmt:
			_, queryOnly = stmtNode.Stmt.(*ast.SelectStmt)
		}
	}

	// log.Warn(fmt.Sprintf("queryOnly: %t", queryOnly))
	if queryOnly {
		// 只对查询语句处理mpp
		result := mctx.PrepareResult()
		// log.Warn(fmt.Sprintf("result is null: %t", result == nil))
		if result != nil {
			params := result.Params()
			var mppValue string
			if value, ok := params[mctech.ParamMPP]; ok {
				mppValue = value.(string)
			}

			// log.Warn("mppValue: " + mppValue)
			if mppValue != "allow" && mppValue != "" {
				mppVarCtx := mctx.(mctech.SessionMPPVarsContext)
				if err = mppVarCtx.StoreSessionMPPVars(mppValue); err != nil {
					return err
				}
				defer mppVarCtx.ReloadSessionMPPVars()
				if err = mppVarCtx.SetSessionMPPVars(mppValue); err != nil {
					return err
				}
			}
		}
	}

	handler := mctech.GetHandler()
	if _, err = handler.ApplyAndCheck(mctx, stmts); err != nil {
		if strFmt, ok := cc.getCtx().Session.(fmt.Stringer); ok {
			logutil.Logger(ctx).Warn("mctech SQL failed", zap.Error(err), zap.Stringer("session", strFmt), zap.String("SQL", sql))
		}

		return err
	}

	return nil
}
