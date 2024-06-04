package interceptor

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type interceptor struct{}

func init() {
	mctech.SetInterceptor(&interceptor{})
}

func (*interceptor) BeforeParseSQL(ctx context.Context, sess sessionctx.Context, sql string) (context.Context, mctech.Context, string, error) {
	subCtx, mctx, err := mctech.WithNewContext3(ctx, sess, false)
	if err != nil {
		return subCtx, nil, "", err
	}

	handler := mctech.GetHandler()
	if mctx != nil {
		if sql, err = handler.PrepareSQL(mctx, sql); err != nil {
			return subCtx, nil, "", err
		}
	}

	return subCtx, mctx, sql, nil
}

func (*interceptor) AfterParseSQL(ctx context.Context, sess sessionctx.Context, mctx mctech.Context, stmt ast.StmtNode) (err error) {
	// 判断当前是否是查询语句
	queryOnly := false
	switch stmtNode := stmt.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		queryOnly = true
	case *ast.MCTechStmt:
		_, queryOnly = stmtNode.Stmt.(*ast.SelectStmt)
	case *ast.ExplainStmt:
		_, queryOnly = stmtNode.Stmt.(*ast.SelectStmt)
	case *ast.PrepareStmt:
		// prapare 语句不在这里处理，在PrepareExec.Next方法内解析待执行语句时才处理
		return nil
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
				if err = mppVarCtx.SetSessionMPPVars(mppValue); err != nil {
					return err
				}
			}
		}
	}

	handler := mctech.GetHandler()
	if _, err = handler.ApplyAndCheck(mctx, stmt); err != nil {
		logutil.Logger(ctx).Warn("mctech SQL failed", zap.Error(err), zap.Object("session", sessionctx.ShortInfo(sess)), zap.String("SQL", stmt.OriginalText()))
		return err
	}

	if opts := config.GetMCTechConfig(); opts.Metrics.QueryLog.Enabled {
		exclude := opts.Metrics.Exclude
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
				return nil
			}
		}

		shouldLog := false
		switch stmt.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt,
			*ast.DeleteStmt, *ast.InsertStmt, *ast.UpdateStmt,
			*ast.PrepareStmt, *ast.ExecuteStmt,
			*ast.NonTransactionalDMLStmt:
			shouldLog = true
		}

		if !shouldLog {
			// 不记录指定类型以外的sql
			return nil
		}

		origSQL := stmt.OriginalText()
		if len(origSQL) > opts.Metrics.QueryLog.MaxLength {
			origSQL = origSQL[0:opts.Metrics.QueryLog.MaxLength]
		}
		logutil.Logger(ctx).Warn("(handleQuery) MCTECH SQL QueryLog", zap.Object("session", sessionctx.ShortInfo(sess)), zap.String("SQL", origSQL))
	}

	return nil
}

func (*interceptor) AfterHandleStmt(ctx context.Context, sess sessionctx.Context, stmt ast.StmtNode, err error) {
	if sessVars := sess.GetSessionVars(); sessVars.InRestrictedSQL {
		// 不记录内部sql
		return
	}

	metrics := &config.GetMCTechConfig().Metrics
	if !metrics.LargeQuery.Enabled && !metrics.SQLTrace.Enabled {
		// 先检查功能是否启用
		return
	}

	var mctx mctech.Context
	var e error
	mctx, e = mctech.GetContext(ctx)
	if e != nil {
		panic(e)
	}
	var dbs []string
	if mctx != nil {
		dbs = mctx.GetDbs(stmt)
	}

	if dbs != nil {
		for _, db := range metrics.Exclude {
			if slices.Contains(dbs, db) {
				// 不记录这些数据库下的sql
				return
			}
		}
	}

	if metrics.LargeQuery.Enabled {
		logLargeQuery(ctx, sess, stmt, err == nil)
	}

	if err == nil {
		// 全量sql只记录执行成功的sql
		if metrics.SQLTrace.Enabled {
			traceFullQuery(ctx, sess)
		}
	}
}

// 记录超长sql
func logLargeQuery(ctx context.Context, sess sessionctx.Context, stmt ast.StmtNode, succ bool) {
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
		execStmt := sess.Value(mctech.MCExecStmtVarKey).(*executor.ExecStmt)
		execStmt.SaveLargeQuery(ctx, sqlType, succ)
	}
}

// 记录全量sql
func traceFullQuery(ctx context.Context, sess sessionctx.Context) {
	sessVars := sess.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	origSQL := stmtCtx.OriginalSQL

	execStmt := sess.Value(mctech.MCExecStmtVarKey).(*executor.ExecStmt)
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
	case *ast.TruncateTableStmt:
		sqlType = "truncate"
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

	timeStart := sessVars.StartTime                                      // 执行sql开始时间（不含从sql字符串解析成语法树的时间）
	connID := sessVars.ConnectionID                                      // SQL 查询客户端连接 ID
	queryTime := time.Since(sessVars.StartTime) + sessVars.DurationParse // 执行 SQL 耗费的自然时间
	parseTime := sessVars.DurationParse                                  // 解析耗时
	compileTime := sessVars.DurationCompile                              // 生成执行计划耗时
	copTime := execDetails.CopTime                                       // Coprocessor 执行耗时
	var _ string                                                         // 移除注释并且参数替换后的sql模板
	memMax := stmtCtx.MemTracker.MaxConsumed()                           // 该 SQL 查询执行时占用的最大内存空间
	diskMax := stmtCtx.DiskTracker.MaxConsumed()                         // 该 SQL 查询执行时占用的最大磁盘空间
	writeSQLRespTotal := stmtDetail.WriteSQLRespDuration                 // 发送结果耗时
	firstRowReadyTime := queryTime - writeSQLRespTotal                   // 首行结果准备好时间(总执行时间除去发送结果耗时)
	resultRows := executor.GetResultRowsCount(stmtCtx, execStmt.Plan)    // 查询返回结果行数
	affectedRows := stmtCtx.AffectedRows()                               // sql执行结果影响的数据行数
	var writeKeys int = 0                                                // 写入 Key 个数
	if execDetails.CommitDetail != nil {
		writeKeys = execDetails.CommitDetail.WriteKeys
	}
	_, digest := stmtCtx.SQLDigest() //

	failpoint.Inject("MockTraceLogData", func(val failpoint.Value) {
		values := make(map[string]any)
		err := json.Unmarshal([]byte(val.(string)), &values)
		if err != nil {
			panic(err)
		}

		for k, v := range values {
			switch k {
			case "startedAt":
				if t, err := time.ParseInLocation("2006-01-02 15:04:05.000", v.(string), time.Local); err == nil {
					timeStart = t
					queryTime, _ = time.ParseDuration("3.315821ms")
					parseTime, _ = time.ParseDuration("176.943µs")
					compileTime, _ = time.ParseDuration("1.417613ms")
					copTime, _ = time.ParseDuration("0s128ms")
					firstRowReadyTime, _ = time.ParseDuration("2.315821ms")
					writeSQLRespTotal, _ = time.ParseDuration("1ms")
				}
			case "mem":
				memMax = int64(v.(float64))
			case "disk":
				diskMax = int64(v.(float64))
			case "keys":
				writeKeys = int(v.(float64))
			case "rows":
				resultRows = int64(v.(float64))
			case "affected":
				affectedRows = uint64(v.(float64))
			}
		}
	})

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

	var (
		dbs    string // 执行的sql中用到的所有数据库名称列表。','分隔
		tenant string // 所属租户信息
	)
	if mctx, err := mctech.GetContext(ctx); err != nil {
		panic(err)
	} else {
		lst := mctx.GetDbs(execStmt.StmtNode)
		if len(lst) > 0 {
			dbs = strings.Join(lst, ",")
		}
		if result := mctx.PrepareResult(); result != nil {
			tenant = result.Tenant()
		}
	}

	si := sessionctx.ShortInfo(sess)
	var fields = []zapcore.Field{
		zap.String("db", si.GetDB()),
		zap.String("dbs", dbs),
		zap.String("usr", si.GetUser()),
		zap.String("tenant", tenant),
		zap.String("conn", encode(connID)),
		zap.String("tp", sqlType),
		zap.String("at", timeStart.Format("2006-01-02 15:04:05.000")),
		zap.Object("time", &logTimeObject{
			all:   queryTime,
			parse: parseTime,
			plan:  compileTime,
			cop:   copTime,
			ready: firstRowReadyTime,
			send:  writeSQLRespTotal,
		}),
		zap.Stringer("digest", digest),
		zap.Int64("mem", memMax),
		zap.Int64("disk", diskMax),
		zap.Int("keys", writeKeys),
		zap.Uint64("affected", affectedRows),
		zap.Int64("rows", resultRows),
	}

	fields = append(fields, zap.String("sql", origSQL))
	if len(zip) > 0 {
		fields = append(fields, zap.Binary("zip", zip))
	}

	renderTraceLog(sess, fields)
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
