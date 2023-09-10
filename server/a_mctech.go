// add by zhangbing

package server

import (
	// 强制初始化preps

	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/mctech"
	_ "github.com/pingcap/tidb/mctech/preps"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
)

func (cc *clientConn) beforeParseSql(ctx context.Context, sql string) (context.Context, mctech.Context, string, error) {
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

	handler := mctech.GetHandler()
	if _, err = handler.ApplyAndCheck(mctx, stmts); err != nil {
		db, user, client := sessionctx.ResolveSession(cc.getCtx())
		logutil.Logger(ctx).Warn("mctech SQL failed", zap.Error(err),
			zap.String("token", mctech.LogFilterToken),
			zap.String("db", db), zap.String("user", user), zap.String("client", client),
			zap.String("SQL", sql))
		return err
	}

	if opts := config.GetMCTechConfig(); opts.Metrics.SqlLog.Enabled {
		exclude := opts.Metrics.Exclude
		for _, stmt := range stmts {
			dbs := mctx.GetDbs(stmt)
			if dbs != nil && len(exclude) > 0 {
				ignore := false
				for _, db := range exclude {
					if slices.Contains(dbs, db) {
						// 不记录这些数据库下的sql
						ignore = true
						break
					}
				}

				if ignore {
					break
				}
			}

			shouldLog := false
			switch stmt.(type) {
			case *ast.SelectStmt, *ast.SetOprStmt,
				*ast.DeleteStmt, *ast.InsertStmt, *ast.UpdateStmt,
				*ast.PrepareStmt,
				*ast.NonTransactionalDMLStmt:
				shouldLog = true
			}

			if !shouldLog {
				// 不记录指定类型以外的sql
				break
			}

			origSql := stmt.OriginalText()
			if len(origSql) > opts.Metrics.SqlLog.MaxLength {
				origSql = origSql[0:opts.Metrics.SqlLog.MaxLength]
			}
			db, user, client := sessionctx.ResolveSession(cc.getCtx())
			logutil.Logger(ctx).Warn("MCTECH SQL handleQuery",
				zap.String("token", mctech.LogFilterToken),
				zap.String("db", db), zap.String("user", user), zap.String("client", client),
				zap.String("SQL", origSql))
		}
	}

	return nil
}

func (cc *clientConn) afterHandleStmt(ctx context.Context, stmt ast.StmtNode, err error) {
	if err != nil {
		// 只记录执行成功的sql
		return
	}

	var mctx mctech.Context
	mctx, err = mctech.GetContext(ctx)
	if err != nil {
		panic(err)
	}

	opts := config.GetMCTechConfig()
	var dbs []string
	if mctx != nil {
		dbs = mctx.GetDbs(stmt)
	}

	if dbs != nil {
		for _, db := range opts.Metrics.Exclude {
			if slices.Contains(dbs, db) {
				// 不记录这些数据库下的sql
				return
			}
		}
	}

	if opts.Metrics.LargeQuery.Enabled {
		cc.logLargeQuery(ctx, stmt, err == nil)
	}
	if opts.Metrics.SqlTrace.Enabled {
		cc.traceFullQuery(ctx, stmt)
	}
}

// 记录超长sql
func (cc *clientConn) logLargeQuery(ctx context.Context, stmt ast.StmtNode, succ bool) {
	opts := config.GetMCTechConfig()
	sqlType := "other"
	switch stmt.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		sqlType = "select"
	case *ast.DeleteStmt:
		sqlType = "delete"
	case *ast.InsertStmt:
		sqlType = "insert"
	case *ast.UpdateStmt:
		sqlType = "update"
	}

	// 捕获后续执行的异常，不再向外抛出
	defer func() {
		if err := recover(); err != nil {
			logutil.Logger(ctx).Warn("[logLargeQuery] 记录大sql信息出错", zap.Error(err.(error)), zap.Stack("stack"))
		}
	}()

	if slices.Contains(opts.Metrics.LargeQuery.SqlTypes, sqlType) {
		execStmt := cc.ctx.Value(sessionctx.MCTechExecStmtVarKey).(*executor.ExecStmt)
		execStmt.SaveLargeQuery(ctx, succ)
	}
}

// 记录全量sql
func (cc *clientConn) traceFullQuery(ctx context.Context, stmt ast.StmtNode) {
	sessVars := cc.ctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	origSql := stmt.OriginalText()

	// 是否为内部sql查询
	internal := sessVars.InRestrictedSQL
	if internal {
		// FIXME: 仅调试代码
		logutil.Logger(ctx).Warn("内部sql查询", zap.String("SQL", origSql))
		return
	}

	var sqlType string // sql语句类型
	switch s := stmt.(type) {
	case *ast.PrepareStmt, *ast.ExecuteStmt, *ast.DeallocateStmt: // execute
		sqlType = "exec"
	case *ast.BeginStmt, *ast.RollbackStmt, *ast.CommitStmt: // transaction
		sqlType = "tx"
	case *ast.NonTransactionalDMLStmt:
		switch s.DMLStmt.(type) {
		case *ast.DeleteStmt:
			sqlType = "delete"
		case *ast.UpdateStmt:
			sqlType = "update"
		case *ast.InsertStmt:
			sqlType = "insert"
		}
	case *ast.SelectStmt, *ast.SetOprStmt: // select
		sqlType = "select"
	case *ast.DeleteStmt: // delete
		sqlType = "delete"
	case *ast.InsertStmt: // insert
		sqlType = "insert"
	case *ast.UpdateStmt: // update
		sqlType = "update"
	case *ast.LockTablesStmt, *ast.UnlockTablesStmt, // lock/unlock table
		// *ast.UseStmt,  // use
		*ast.CallStmt, // precedure
		*ast.DoStmt:   // do block
		sqlType = "misc"
	default:
		return
	}

	// 捕获后续执行的异常，不再向外抛出
	defer func() {
		if err := recover(); err != nil {
			logutil.Logger(ctx).Warn("[traceFullQuery] 记录sql执行信息出错", zap.Error(err.(error)), zap.Stack("stack"))
		}
	}()

	execDetails := stmtCtx.GetExecDetails()

	var stmtDetail execdetails.StmtExecDetails
	execStmt := cc.ctx.Value(sessionctx.MCTechExecStmtVarKey).(*executor.ExecStmt)
	stmtDetailRaw := execStmt.GoCtx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
	}

	timeStart := sessVars.StartTime // 执行sql开始时间（不含从sql字符串解析成语法树的时间）
	var db string                   // 执行该 SQL 查询时使用的数据库名称
	// connId := sessVars.ConnectionID                                          // SQL 查询客户端连接 ID
	queryTime := time.Since(sessVars.StartTime) + sessVars.DurationParse // 执行 SQL 耗费的自然时间
	parseTime := sessVars.DurationParse                                  // 解析耗时
	compileTime := sessVars.DurationCompile                              // 生成执行计划耗时
	copTime := execDetails.CopTime                                       // Coprocessor 执行耗时
	var _ string                                                         // 移除注释并且参数替换后的sql模板
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
	normalizedSQL, digest := stmtCtx.SQLDigest() //

	var zip []byte
	sqlLen := len(origSql)
	if sqlLen > config.GetMCTechConfig().Metrics.SqlTrace.CompressThreshold {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		if _, err := gz.Write([]byte(origSql)); err == nil {
			if err := gz.Flush(); err == nil {
				if err := gz.Close(); err == nil {
					zip = b.Bytes()
					origSql = normalizedSQL[:128] + fmt.Sprintf("...len(%d)", sqlLen)
				} else {
					log.Error("trace sql error", zap.Error(err))
				}
			}
		} else {
			log.Error("trace sql error", zap.Error(err))
		}
	}

	var fields = []zapcore.Field{
		zap.String("db", db),
		zap.String("usr", user),
		// zap.Uint64("conn", connId),
		zap.String("tp", sqlType),
		zap.String("at", timeStart.Format("2006-01-02 15:04:05.000")),
		zap.Object("time", &mctech.LobTimeObject{
			All:   queryTime,
			Parse: parseTime,
			Plan:  compileTime,
			Cop:   copTime,
			Ready: firstRowReadyTime,
			Send:  writeSQLRespTotal,
		}),
		zap.Stringer("digest", digest),
		zap.Int64("mem", memMax),
		zap.Int64("disk", diskMax),
		zap.Int("keys", writeKeys),
		zap.Int64("rows", resultRows),
		zap.String("sql", origSql),
	}

	if len(zip) > 0 {
		fields = append(fields, zap.Binary("zip", zip))
	}

	mctech.F().Info(
		"", // 忽略Message字段
		fields...,
	)
}
