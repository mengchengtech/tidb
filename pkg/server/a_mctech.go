// add by zhangbing

package server

import (
	// 强制初始化preps

	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/mctech"
	_ "github.com/pingcap/tidb/pkg/mctech/preps"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

func (cc *clientConn) beforeParseSql(ctx context.Context, sql string) (context.Context, mctech.Context, string, error) {
	if opts := mctech.GetOption(); opts.QueryLogEnabled {
		if len(sql) > opts.QueryLogMaxLength {
			sql = sql[0:opts.QueryLogMaxLength]
		}
		db, user, client := sessionctx.ResolveSession(cc.getCtx())
		logutil.Logger(ctx).Warn("MCTECH SQL handleQuery",
			zap.String("token", mctech.LogFilterToken),
			zap.String("db", db), zap.String("user", user), zap.String("client", client),
			zap.String("SQL", sql))
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
	sqlType := "other"
	for _, stmt := range stmts {
		switch stmtNode := stmt.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt:
			queryOnly = true
			sqlType = "select"
		case *ast.DeleteStmt:
			sqlType = "delete"
		case *ast.InsertStmt:
			sqlType = "insert"
		case *ast.UpdateStmt:
			sqlType = "update"
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
				if err = mppVarCtx.StoreSessionMPPVars(ctx, mppValue); err != nil {
					return err
				}
				defer mppVarCtx.ReloadSessionMPPVars()
				if err = mppVarCtx.SetSessionMPPVars(mppValue); err != nil {
					return err
				}
			}
		}
	}

	if opts := mctech.GetOption(); opts.LargeQueryEnabled {
		if slices.Contains(opts.LargeQueryTypes, sqlType) && len(sql) > opts.LargeQueryThreshold {
			// TODO: 记录超长sql
			panic(fmt.Errorf("not implemented"))
		}
	}

	handler := mctech.GetHandler()
	if _, err = handler.ApplyAndCheck(mctx, stmts); err != nil {
		db, user, client := sessionctx.ResolveSession(cc.getCtx())
		logutil.Logger(ctx).Warn("mctech SQL failed", zap.Error(err),
			zap.String("token", mctech.LogFilterToken),
			zap.String("db", db), zap.String("user", user), zap.String("client", client),
			zap.String("SQL", sql))
		return err
	}

	return nil
}

func (cc *clientConn) afterHandleStmt(ctx context.Context, stmt ast.StmtNode, err error) {
	if err != nil {
		// 只记录执行成功的sql
		return
	}

	opts := mctech.GetOption()
	if !opts.SqlTraceEnabled {
		return
	}

	sessVars := cc.ctx.GetSessionVars()
	origSql := stmt.OriginalText()
	// 是否为内部sql查询
	internal := sessVars.InRestrictedSQL
	if internal {
		// FIXME: 仅调试代码
		logutil.Logger(ctx).Warn("内部sql查询", zap.String("SQL", origSql))
		return
	}

	// 捕获后续执行的异常，不再向外抛出
	defer func() {
		if err := recover(); err != nil {
			logutil.Logger(ctx).Warn("记录sql执行信息出错", zap.Error(err.(error)))
		}
	}()

	switch stmt.(type) {
	case *ast.UseStmt, // use
		*ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt, // execute
		*ast.BeginStmt, *ast.RollbackStmt, *ast.CommitStmt, // transaction
		*ast.NonTransactionalDMLStmt,
		*ast.SelectStmt, *ast.SetOprStmt, // select
		*ast.DeleteStmt,                            // delete
		*ast.InsertStmt,                            // insert
		*ast.UpdateStmt,                            // update
		*ast.LockTablesStmt, *ast.UnlockTablesStmt, // lock/unlock table
		*ast.CallStmt, // precedure
		*ast.DoStmt:   // do block
		break
	default:
		return
	}
	var mctx mctech.Context
	mctx, err = mctech.GetContext(ctx)
	if err != nil {
		panic(err)
	}

	var dbs []string
	if mctx != nil {
		dbs = mctx.GetDbs(stmt)
	}

	if dbs != nil {
		for _, db := range opts.SqlTraceIgnoreDbs {
			if slices.Contains(dbs, db) {
				// 不记录这些数据库下的sql
				return
			}
		}
	}

	stmtCtx := sessVars.StmtCtx
	execDetails := stmtCtx.GetExecDetails()

	var stmtDetail execdetails.StmtExecDetails
	execStmt := cc.ctx.Value(sessionctx.MCTechExecStmtVarKey).(*executor.ExecStmt)
	stmtDetailRaw := execStmt.GoCtx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
	}

	timeStart := sessVars.StartTime                                      // 执行sql开始时间（不含从sql字符串解析成语法树的时间）
	timeEnd := time.Now()                                                // 该 SQL 查询结束运行时的时间
	var db string                                                        // 执行该 SQL 查询时使用的数据库名称
	connId := sessVars.ConnectionID                                      // SQL 查询客户端连接 ID
	queryTime := time.Since(sessVars.StartTime) + sessVars.DurationParse // 执行 SQL 耗费的自然时间
	parseTime := sessVars.DurationParse                                  // 解析耗时
	compileTime := sessVars.DurationCompile                              // 生成执行计划耗时
	copTime := execDetails.CopTime                                       // Coprocessor 执行耗时
	var _ string                                                         // 移除注释并且参数替换后的sql模板
	var digest *parser.Digest                                            // SQL 模板的唯一标识（SQL 指纹）
	_, digest = stmtCtx.SQLDigest()                                      //
	memMax := stmtCtx.MemTracker.MaxConsumed()                           // 该 SQL 查询执行时占用的最大内存空间
	diskMax := stmtCtx.DiskTracker.MaxConsumed()                         // 该 SQL 查询执行时占用的最大磁盘空间
	var user string                                                      // 执行该 SQL 查询的用户名，可能存在多个执行用户，仅显示其中某一个
	var _ string                                                         // 发送 SQL 查询的客户端地址
	db, user, _ = sessionctx.ResolveSession(cc.getCtx())                 //
	writeSQLRespTotal := stmtDetail.WriteSQLRespDuration                 // 发送结果耗时
	firstRowReadyTime := queryTime - writeSQLRespTotal                   // 首行结果准备好时间(总执行时间除去发送结果耗时)
	resultRows := executor.GetResultRowsCount(stmtCtx, execStmt.Plan)    // 查询返回结果行数
	var writeKeys int = 0                                                // 写入 Key 个数
	if execDetails.CommitDetail != nil {
		writeKeys = execDetails.CommitDetail.WriteKeys
	}

	if len(origSql) > mctech.GetOption().SqlTraceCompressThreshold {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		if _, err := gz.Write([]byte(origSql)); err == nil {
			if err := gz.Flush(); err == nil {
				if err := gz.Close(); err == nil {
					origSql = base64.StdEncoding.EncodeToString(b.Bytes())
				}
			}
		}
	}

	mctech.F().Warn("", // 忽略Message字段
		zap.String("DB", db),
		zap.String("User", user),
		zap.Uint64("ConnId", connId),
		zap.String("Start", timeStart.Format(time.RFC3339Nano)),
		zap.String("End", timeEnd.Format(time.RFC3339Nano)),
		zap.Duration("QueryTime", queryTime),
		zap.Duration("ParseTime", parseTime),
		zap.Duration("CompileTime", compileTime),
		zap.Duration("CopTime", copTime),
		zap.Stringer("RenderTime", writeSQLRespTotal),
		zap.Duration("FirstRowTime", firstRowReadyTime),
		zap.Stringer("Digest", digest),
		zap.Int64("MemMax", memMax),
		zap.Int64("DiskMax", diskMax),
		zap.Int("WriteKeys", writeKeys),
		zap.Int64("ResultRows", resultRows),
		zap.String("SQL", origSql),
	)
}