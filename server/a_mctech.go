// add by zhangbing

package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/mctech"

	// 强制初始化preps
	_ "github.com/pingcap/tidb/mctech/preps"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
)

func (cc *clientConn) beforeParseSQL(ctx context.Context, sql string) (context.Context, mctech.Context, string, error) {
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

func (cc *clientConn) afterParseSQL(ctx context.Context, mctx mctech.Context, sql string, stmts []ast.StmtNode) (err error) {
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
		logutil.Logger(ctx).Warn("mctech SQL failed", zap.Error(err), zap.Object("session", sessionctx.ShortInfo(cc.getCtx())), zap.String("SQL", sql))
		return err
	}

	if opts := config.GetMCTechConfig(); opts.Metrics.QueryLog.Enabled {
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

			origSQL := stmt.OriginalText()
			if len(origSQL) > opts.Metrics.QueryLog.MaxLength {
				origSQL = origSQL[0:opts.Metrics.QueryLog.MaxLength]
			}
			logutil.Logger(ctx).Warn("(handleQuery) MCTECH SQL QueryLog", zap.Object("session", sessionctx.ShortInfo(cc.getCtx())), zap.String("SQL", origSQL))
		}
	}

	return nil
}

func (cc *clientConn) afterHandleStmt(ctx context.Context, stmt ast.StmtNode, err error) {
	if sessVars := cc.ctx.GetSessionVars(); sessVars.InRestrictedSQL {
		// 不记录内部sql
		return
	}

	var mctx mctech.Context
	var e error
	mctx, e = mctech.GetContext(ctx)
	if e != nil {
		panic(e)
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

	if err == nil {
		// 全量sql只记录执行成功的sql
		if opts.Metrics.SQLTrace.Enabled {
			cc.traceFullQuery(ctx)
		}
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

	if slices.Contains(opts.Metrics.LargeQuery.Types, sqlType) {
		execStmt := cc.ctx.Value(sessionctx.MCTechExecStmtVarKey).(*executor.ExecStmt)
		execStmt.SaveLargeQuery(ctx, sqlType, succ)
	}
}

// 记录全量sql
func (cc *clientConn) traceFullQuery(ctx context.Context) {
	sessVars := cc.ctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	origSQL := stmtCtx.OriginalSQL

	execStmt := cc.ctx.Value(sessionctx.MCTechExecStmtVarKey).(*executor.ExecStmt)
	var sqlType string // sql语句类型
	switch s := execStmt.StmtNode.(type) {
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
	case *ast.LoadDataStmt:
		sqlType = "load"
	case *ast.SetStmt:
		sqlType = "set"
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
	stmtDetailRaw := execStmt.GoCtx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
	}

	timeStart := sessVars.StartTime                                          // 执行sql开始时间（不含从sql字符串解析成语法树的时间）
	connID := sessVars.ConnectionID                                          // SQL 查询客户端连接 ID
	queryTime := time.Since(sessVars.StartTime) + sessVars.DurationParse     // 执行 SQL 耗费的自然时间
	parseTime := sessVars.DurationParse                                      // 解析耗时
	compileTime := sessVars.DurationCompile                                  // 生成执行计划耗时
	copTime := execDetails.CopTime                                           // Coprocessor 执行耗时
	var _ string                                                             // 移除注释并且参数替换后的sql模板
	memMax := stmtCtx.MemTracker.MaxConsumed()                               // 该 SQL 查询执行时占用的最大内存空间
	diskMax := stmtCtx.DiskTracker.MaxConsumed()                             // 该 SQL 查询执行时占用的最大磁盘空间
	writeSQLRespTotal := stmtDetail.WriteSQLRespDuration                     // 发送结果耗时
	firstRowReadyTime := queryTime - writeSQLRespTotal                       // 首行结果准备好时间(总执行时间除去发送结果耗时)
	resultRows := executor.GetResultRowsCount(stmtCtx, execStmt.Plan) // 查询返回结果行数
	var writeKeys int = 0                                                    // 写入 Key 个数
	if execDetails.CommitDetail != nil {
		writeKeys = execDetails.CommitDetail.WriteKeys
	}
	_, digest := stmtCtx.SQLDigest() //

	var zip []byte
	sqlLen := len(origSQL)
	if sqlLen > config.GetMCTechConfig().Metrics.SQLTrace.CompressThreshold {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		if _, err := gz.Write([]byte(origSQL)); err == nil {
			if err := gz.Flush(); err == nil {
				if err := gz.Close(); err == nil {
					zip = b.Bytes()
					origSQL = origSQL[:256] + fmt.Sprintf("...len(%d)", sqlLen)
				} else {
					log.Error("trace sql error", zap.Error(err))
				}
			}
		} else {
			log.Error("trace sql error", zap.Error(err))
		}
	}

	si := sessionctx.ShortInfo(cc.getCtx()) //
	var fields = []zapcore.Field{
		zap.String("db", si.GetDB()),
		zap.String("usr", si.GetUser()),
		zap.String("conn", encode(connID)),
		zap.String("tp", sqlType),
		zap.String("at", timeStart.Format("2006-01-02 15:04:05.000")),
		zap.Object("time", &mctech.LogTimeObject{
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
		zap.String("sql", origSQL),
	}

	if len(zip) > 0 {
		fields = append(fields, zap.Binary("zip", zip))
	}

	mctech.F().Info(
		"", // 忽略Message字段
		fields...,
	)
}

const chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func encode(num uint64) string {
	bytes := []byte{}
	for num > 0 {
		bytes = append(bytes, chars[num%62])
		num = num / 62
	}

	for left, right := 0, len(bytes)-1; left < right; left, right = left+1, right-1 {
		bytes[left], bytes[right] = bytes[right], bytes[left]
	}

	return string(bytes)
}
