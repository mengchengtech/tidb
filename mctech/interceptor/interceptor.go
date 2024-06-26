package interceptor

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	clientutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/slices"
)

type interceptor struct{}

const timeFormat = "2006-01-02 15:04:05.000"

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

	opts := config.GetMCTechConfig()
	queryLogEnabled := opts.Metrics.QueryLog.Enabled
	handler := mctech.GetHandler()
	if _, err = handler.ApplyAndCheck(mctx, stmt); err != nil {
		if queryLogEnabled {
			logutil.BgLogger().Warn("mctech SQL failed", zap.Error(err), zap.Object("session", sessionctx.ShortInfo(sctx)), zap.String("SQL", stmt.OriginalText()))
		}
		return err
	}

	if queryLogEnabled {
		if ignoreTrace(sctx, mctx, stmt, &opts.Metrics) {
			return nil
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

// ignoreTrace 是否跳过记录sql跟踪日志
func ignoreTrace(sctx sessionctx.Context, mctx mctech.Context, stmt ast.StmtNode, metrics *config.MctechMetrics) bool {
	databases := metrics.Ignore.ByDatabases
	var dbs []string
	if stmt != nil {
		dbs = mctx.GetDbs(stmt)
	} else {
		dbs = []string{sctx.GetSessionVars().CurrentDB}
	}

	if dbs != nil && len(databases) > 0 {
		for _, db := range databases {
			if slices.Contains(dbs, db) {
				// 不记录这些数据库下的sql
				return true
			}
		}
	}

	for _, r := range sctx.GetSessionVars().ActiveRoles {
		if slices.Contains(metrics.Ignore.ByRoles, r.Username) {
			// 不记录这些角色执行的sql
			return true
		}
	}
	return false
}

func doAfterHandleStmt(sctx sessionctx.Context, sql string, stmt ast.StmtNode, err error) {
	sessVars := sctx.GetSessionVars()
	if sessVars.InRestrictedSQL {
		// 不记录内部sql
		return
	}

	metrics := &config.GetMCTechConfig().Metrics
	if !metrics.LargeQuery.Enabled && !metrics.SQLTrace.Enabled {
		// 先检查功能是否启用
		return
	}

	mctx := mctech.GetContextStrict(sctx)
	if ignoreTrace(sctx, mctx, stmt, metrics) {
		return
	}

	var execStmt *executor.ExecStmt
	if v := sctx.Value(mctech.MCExecStmtVarKey); v != nil {
		execStmt = v.(*executor.ExecStmt)
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
		origSQL = sql // 只有当stmt为nil时才会使用传入的sql参数，此时代表的是 sql 解析失败
		info    *sqlStmtInfo
	)
	// 此处不能使用 sessVars.StmtCtx 获取sql信息
	// 原因参考当前方法后续 `if execStmt != nil {......}` 块内部的说明
	//
	// stmt 是解析sql后拆分的一条一条独立的sql语法树对象，与当前sql是密切相关的
	if stmt != nil {
		origSQL = stmt.OriginalText()
	}
	if info = getSQLStmtInfo(stmt, sctx.GetSessionVars()); info == nil {
		// 返回空值表示不记录trace 日志
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
		connID  uint64         // SQL 查询客户端连接 ID
		db      = si.GetDB()   // 执行sql时的当前库名称
		dbs     string         // 执行的sql中用到的所有数据库名称列表。
		inTX    bool           // 当前sql是否在事务中
		user    = si.GetUser() // 执行sql时使用的账号
		tenant  string         // 所属租户信息
		at      time.Time      // 执行sql开始时间，即创建mctech.Context的时间
		txID    uint64         // 事务号(显示事务和隐式事务)
		maxAct  int64          // sql执行过程中读取/生成的最大行数（与rows不一样，中间过程生成的行数多不代表结果集中的行数多）
		rows    int64          // 查询返回结果行数
		memMax  int64          // 该 SQL 查询执行时占用的最大内存空间
		diskMax int64          // 该 SQL 查询执行时占用的最大磁盘空间
		digest  string         // sql 语句模板的hash
		across  string         // sql中指定的跨库查询的数据库
		zip     []byte         // 压缩后的sql文本

		times  logTimeObject    // 各种时间信息
		maxCop *logMaxCopObject // tikv coprocessor相关的信息
		tx     *logTXObject     // 修改数据相关的信息
		ru     logRUStatObject  // 当前sql资源消耗信息

		warnings *logWarningObjects // 执行中生成的警告信息
	)

	txID = sessVars.TxnCtx.StartTS
	var stmtDetail execdetails.StmtExecDetails
	if execStmt != nil {
		stmtDetailRaw := execStmt.GoCtx.Value(execdetails.StmtExecDetailKey)
		if stmtDetailRaw != nil {
			stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
			times.send = stmtDetail.WriteSQLRespDuration
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
				rows = executor.GetResultRowsCount(stmtCtx, plan)
			}
			// 添加开发辅助代码
			if stmtCtx.RuntimeStatsColl != nil {
				collector := newPlanStatCollector(stmtCtx)
				ct := collector.collect()
				times.tidb = ct.tidbTime
				// times.tikv.process2 = ct.tikvCopTime
				times.cop.tiflash = ct.tiflashCopTime
				maxAct = ct.maxActRows
				if rs, ok := sctx.Value(mctech.MCRUDetailsCtxKey).(*clientutil.RURuntimeStats); ok {
					ru.rru, ru.wru = rs.RRU(), rs.WRU()
				}
			}
			warnings = newWarnings(executor.CollectWarnings(stmtCtx))
			if cd := stmtCtx.CopTasksDetails(); cd != nil {
				maxCop = &logMaxCopObject{
					procAddr: cd.MaxProcessAddress,
					procTime: cd.MaxProcessTime,
					tasks:    cd.NumCopTasks,
				}
			}

			execDetails := stmtCtx.GetExecDetails()
			times.cop.wall = execDetails.CopTime
			times.cop.tikv = execDetails.TimeDetail.ProcessTime
			memMax = stmtCtx.MemTracker.MaxConsumed()
			diskMax = stmtCtx.DiskTracker.MaxConsumed()

			if info.modified {
				tx = &logTXObject{affected: stmtCtx.AffectedRows()}
			}
			if cd := execDetails.CommitDetail; cd != nil {
				if info.modified {
					tx.keys, tx.size = cd.WriteKeys, cd.WriteSize
					times.tx = &txTimeObject{prewrite: cd.PrewriteTime, commit: cd.CommitTime}
				}
			}
			_, d := stmtCtx.SQLDigest()
			digest = d.String()
		}
	}

	mctx := mctech.GetContextStrict(sctx)
	at = mctx.StartedAt()
	connID = sessVars.ConnectionID
	inTX = sessVars.InTxn()
	times.all = time.Since(at)
	times.parse = sessVars.DurationParse
	times.plan = sessVars.DurationCompile
	times.ready = times.all - times.send

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

	var fields = []zapcore.Field{
		zap.String("db", db),
		zap.String("dbs", dbs),
		zap.String("usr", user),
		zap.String("tenant", tenant),
		zap.String("conn", encode(connID)),
		zap.Bool("inTX", inTX),
		zap.String("cat", info.category),
		zap.String("tp", info.sqlType),
		zap.String("across", across),
		zap.String("at", at.Format(timeFormat)),
		zap.String("txId", encode(txID)),
		zap.Int64("maxAct", maxAct),
		zap.String("digest", digest),
		zap.Int64("rows", rows),
		zap.Int64("mem", memMax),
		zap.Int64("disk", diskMax),
		zap.Object("times", &times),
		zap.Object("ru", &ru),
	}
	if maxCop != nil {
		fields = append(fields, zap.Object("maxCop", maxCop))
	}
	if tx != nil {
		fields = append(fields, zap.Object("tx", tx))
	}
	if warnings != nil {
		fields = append(fields, zap.Object("warnings", warnings))
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

func getSQLStmtInfo(stmt ast.StmtNode, sessVars *variable.SessionVars) (info *sqlStmtInfo) {
	switch s := stmt.(type) {
	case *ast.ExecuteStmt:
		var (
			prepStmt *core.PlanCacheStmt
			err      error
		)
		if prepStmt, err = core.GetPreparedStmt(s, sessVars); err != nil {
			panic(err)
		}

		raw := getSQLStmtInfo(prepStmt.PreparedAst.Stmt, sessVars)
		if raw != nil {
			info = &sqlStmtInfo{"exec", raw.sqlType, raw.modified}
		}
	case *ast.PrepareStmt:
		info = sqlPrepareInfo
	case *ast.DeallocateStmt:
		info = sqlDeallocateInfo
	case *ast.BeginStmt:
		info = sqlBeginInfo
	case *ast.RollbackStmt:
		info = sqlRollbackInfo
	case *ast.CommitStmt:
		info = sqlCommitInfo
	case *ast.NonTransactionalDMLStmt:
		switch s.DMLStmt.(type) {
		case *ast.DeleteStmt:
			info = sqlDeleteInfo
		case *ast.UpdateStmt:
			info = sqlUpdateInfo
		case *ast.InsertStmt:
			info = sqlInsertInfo
		}
	case *ast.SelectStmt, *ast.SetOprStmt: // select
		info = sqlSelectInfo
	case *ast.DeleteStmt: // delete
		info = sqlDeleteInfo
	case *ast.InsertStmt: // insert
		info = sqlInsertInfo
	case *ast.UpdateStmt: // update
		info = sqlUpdateInfo
	case *ast.LoadDataStmt:
		info = sqlLoadInfo
	case *ast.TruncateTableStmt:
		info = sqlTruncateInfo
	case *ast.SetStmt:
		info = sqlSetInfo
	case *ast.LockTablesStmt:
		info = sqlLockInfo
	case *ast.UnlockTablesStmt: // lock/unlock table
		info = sqlUnlockInfo
	// case *ast.UseStmt: // use
	// 	info = sqlUseInfo
	case *ast.CallStmt: // precedure
		info = sqlCallInfo
	case *ast.DoStmt: // do block
		info = sqlDoInfo
	default:
		// stmt 为 nil 或者除以上各个 case 项以外的类型
	}
	return info
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
