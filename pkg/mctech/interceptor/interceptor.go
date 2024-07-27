package interceptor

import (
	"bytes"
	"compress/gzip"
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
	"github.com/pingcap/tidb/pkg/planner/core"
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

func (*interceptor) BeforeParseSQL(sctx sessionctx.Context, sql string) (mctech.Context, string, error) {
	mctx, err := mctech.WithNewContext(sctx)
	if err != nil {
		return nil, "", err
	}

	handler := mctech.GetHandler()
	if sql, err = handler.PrepareSQL(mctx, sql); err != nil {
		mctx.Clear()
		return nil, "", err
	}

	return mctx, sql, nil
}

func (*interceptor) AfterParseSQL(sctx sessionctx.Context, stmt ast.StmtNode) (err error) {
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

	var mctx mctech.Context
	if mctx, err = mctech.GetContext(sctx); err != nil {
		return err
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
				if err = mppVarCtx.SetSessionMPPVars(mppValue); err != nil {
					return err
				}
			}
		}
	}

	handler := mctech.GetHandler()
	if _, err = handler.ApplyAndCheck(mctx, stmt); err != nil {
		logutil.BgLogger().Warn("mctech SQL failed", zap.Error(err), zap.Object("session", sessionctx.ShortInfo(sctx)), zap.String("SQL", stmt.OriginalText()))
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
		logutil.BgLogger().Warn("(handleQuery) MCTECH SQL QueryLog", zap.Object("session", sessionctx.ShortInfo(sctx)), zap.String("SQL", origSQL))
	}

	return nil
}

func (*interceptor) ParseSQLFailed(sctx sessionctx.Context, sql string, err error) {
	doAfterHandleStmt(sctx, sql, nil, err)
}

func (*interceptor) AfterHandleStmt(sctx sessionctx.Context, stmt ast.StmtNode, err error) {
	doAfterHandleStmt(sctx, "", stmt, err)
}

func doAfterHandleStmt(sctx sessionctx.Context, sql string, stmt ast.StmtNode, err error) {
	if sessVars := sctx.GetSessionVars(); sessVars.InRestrictedSQL {
		// 不记录内部sql
		return
	}

	metrics := &config.GetMCTechConfig().Metrics
	if !metrics.LargeQuery.Enabled && !metrics.SQLTrace.Enabled {
		// 先检查功能是否启用
		return
	}

	var execStmt *executor.ExecStmt
	if v := sctx.Value(mctech.MCExecStmtVarKey); v != nil {
		execStmt = v.(*executor.ExecStmt)
	}

	mctx := mctech.GetContextStrict(sctx)
	var dbs []string
	if stmt != nil {
		dbs = mctx.GetDbs(stmt)
	} else {
		dbs = []string{sctx.GetSessionVars().CurrentDB}
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
		// 只有正确执行的sql才考虑是否记录
		logLargeQuery(execStmt, err == nil)
	}

	if metrics.SQLTrace.Enabled {
		traceFullQuery(sctx, sql, stmt, execStmt, err)
	}
}

// 记录超长sql
func logLargeQuery(execStmt *executor.ExecStmt, succ bool) {
	if execStmt == nil {
		// 某些原因下execStmt为空的时候（比如sql解析失败，未能生成执行计划等），不记录超长sql
		return
	}

	opts := config.GetMCTechConfig()
	sqlType := "other"
	switch execStmt.StmtNode.(type) {
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
			logutil.BgLogger().Warn("[logLargeQuery] 记录大sql信息出错", zap.Error(err.(error)), zap.Stack("stack"))
		}
	}()

	if slices.Contains(opts.Metrics.LargeQuery.Types, sqlType) {
		execStmt.SaveLargeQuery(sqlType, succ)
	}
}

// 记录全量sql
func traceFullQuery(sctx sessionctx.Context, sql string, stmt ast.StmtNode,
	execStmt *executor.ExecStmt, err error) {
	sessVars := sctx.GetSessionVars()
	var (
		sqlType = "unknown" // sql语句类型
		origSQL = sql       // 只有当stmt为nil时才会使用传入的sql参数，此时代表的是 sql 解析失败
	)
	// 此处不能使用 sessVars.StmtCtx 获取sql信息
	// 原因参考当前方法后续 `if execStmt != nil {......}` 块内部的说明
	//
	// stmt 是解析sql后拆分的一条一条独立的sql语法树对象，与当前sql是密切相关的
	if stmt != nil {
		origSQL = stmt.OriginalText()
	}

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
		// stmt 为 nil 或者除以上各个 case 项以外的类型
		return
	}

	// 捕获后续执行的异常，不再向外抛出
	defer func() {
		if err := recover(); err != nil {
			logutil.BgLogger().Warn("[traceFullQuery] 记录sql执行信息出错", zap.Error(err.(error)), zap.Stack("stack"))
		}
	}()

	si := sessionctx.ShortInfo(sctx)
	var (
		timeStart         time.Time      // 执行sql开始时间（不含从sql字符串解析成语法树的时间）
		connID            uint64         // SQL 查询客户端连接 ID
		db                = si.GetDB()   // 执行sql时的当前库名称
		dbs               string         // 执行的sql中用到的所有数据库名称列表。','分隔
		user              = si.GetUser() // 执行sql时使用的账号
		tenant            string         // 所属租户信息
		queryTime         time.Duration  // 执行 SQL 耗费的自然时间
		parseTime         time.Duration  // 解析耗时
		compileTime       time.Duration  // 生成执行计划耗时
		copTime           time.Duration  // Coprocessor 执行耗时
		memMax            int64          // 该 SQL 查询执行时占用的最大内存空间
		diskMax           int64          // 该 SQL 查询执行时占用的最大磁盘空间
		writeSQLRespTotal time.Duration  // 发送结果耗时
		firstRowReadyTime time.Duration  // 首行结果准备好时间(总执行时间除去发送结果耗时)
		resultRows        int64          // 查询返回结果行数
		affectedRows      uint64         // sql执行结果影响的数据行数
		digest            string         // sql 语句的hash
		zip               []byte         // 压缩后的sql文本
		writeKeys         = 0            // 写入 Key 个数
		across            string         // sql中指定的跨库查询的数据库
	)

	var stmtDetail execdetails.StmtExecDetails
	if execStmt != nil {
		stmtDetailRaw := execStmt.GoCtx.Value(execdetails.StmtExecDetailKey)
		if stmtDetailRaw != nil {
			stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
			writeSQLRespTotal = stmtDetail.WriteSQLRespDuration
		}

		// sessVars.StmtCtx 的值不一定准确
		// 这个值只会在session.ExecuteStmt方法被调用时，在该方法内部修改
		// 如果上述方法没有执行到时，比如遇到sql语法错误，还没有解析成ast.StmtNode对象时，
		// 此时触发全量sql记录进入到当前方法时，sessVars.StmtCtx保存的还是上一次执行ExecuteStmt方法设置的值
		//
		// 另一方面 传入的 execStmt 参数的实例也是在 session.ExecuteStmt 方法内部创建的, 并且创建时间还在 StmtCtx 重置状态后。
		// 因此可以认为只要 execStmt 有值，则StmtCtx的值一定也是最新的，反之当execStmt为nil时，StmtCtx 的状态不确定，此时不能使用
		if stmtCtx := sessVars.StmtCtx; stmtCtx != nil {
			if plan, ok := stmtCtx.GetPlan().(core.Plan); ok {
				resultRows = executor.GetResultRowsCount(stmtCtx, plan)
			}
			execDetails := stmtCtx.GetExecDetails()
			copTime = execDetails.CopTime
			memMax = stmtCtx.MemTracker.MaxConsumed()
			diskMax = stmtCtx.DiskTracker.MaxConsumed()
			affectedRows = stmtCtx.AffectedRows()
			if execDetails.CommitDetail != nil {
				writeKeys = execDetails.CommitDetail.WriteKeys
			}
			_, d := stmtCtx.SQLDigest()
			digest = d.String()
		}
	}

	timeStart = sessVars.StartTime
	connID = sessVars.ConnectionID
	queryTime = time.Since(sessVars.StartTime) + sessVars.DurationParse
	parseTime = sessVars.DurationParse
	compileTime = sessVars.DurationCompile
	firstRowReadyTime = queryTime - writeSQLRespTotal

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

	if mctx, err := mctech.GetContext(sctx); err != nil {
		panic(err)
	} else {
		lst := mctx.GetDbs(stmt)
		if len(lst) > 0 {
			dbs = strings.Join(lst, ",")
		}
		if result := mctx.PrepareResult(); result != nil {
			tenant = result.Tenant()
			if params := result.Params(); params != nil {
				if v, ok := params[mctech.ParamAcross].(string); ok {
					across = v
				}
			}
		}
	}

	var fields = []zapcore.Field{
		zap.String("db", db),
		zap.String("dbs", dbs),
		zap.String("usr", user),
		zap.String("tenant", tenant),
		zap.String("conn", encode(connID)),
		zap.String("tp", sqlType),
		zap.String("across", across),
		zap.String("at", timeStart.Format("2006-01-02 15:04:05.000")),
		zap.Object("time", &logTimeObject{
			all:   queryTime,
			parse: parseTime,
			plan:  compileTime,
			cop:   copTime,
			ready: firstRowReadyTime,
			send:  writeSQLRespTotal,
		}),
		zap.String("digest", digest),
		zap.Int64("mem", memMax),
		zap.Int64("disk", diskMax),
		zap.Int("keys", writeKeys),
		zap.Uint64("affected", affectedRows),
		zap.Int64("rows", resultRows),
	}
	if err != nil {
		fields = append(fields, zap.String("error", err.Error()))
	}

	fields = append(fields, zap.String("sql", origSQL))
	if len(zip) > 0 {
		fields = append(fields, zap.Binary("zip", zip))
	}

	renderTraceLog(sctx, fields)
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
