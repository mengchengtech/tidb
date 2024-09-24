package interceptor

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MustCompressForTest 仅供测试代码使用
func MustCompressForTest(sqlLen int, threshold int) (int, int, bool) {
	return mustCompress(sqlLen, threshold)
}

// mustCompress SQL记录日志时是否需要压缩存储
// :params sqlLen:int 原始sql长度
func mustCompress(sqlLen int, threshold int) (int, int, bool) {
	if sqlLen <= threshold {
		// 不需要压缩
		return -1, -1, false
	}

	// sql 语句中的一些重要信息一般在开始和结束位置，预留一部分给sql末尾的字符串
	// 当 threshold >= sqlReserveBothLen 时。
	// * prefixEnd=threshold-sqlSuffixLen, suffixStart=sqlLen-sqlSuffixLen
	//
	// 当 sqlReserveBothLen > threshold >= sqlPrefixLen 时，优先保证前面的字符串（长度为sqlPrefixLen），不足部分从后面的字符串长度扣除（实际长度为threshold - sqlPrefixLen）
	// * prefixEnd=sqlPrefixLen, suffixStart=sqlLen-(threshold-sqlPrefixLen)
	//
	// 当 sqlPrefixLen > threshold 时，prefixEnd取值为实际的threshold（长度为threshold），suffixStart始终为sqlLen（长度为0）
	// * prefixEnd=threshold, suffixStart=sqlLen

	// 截取sql最前面字符串的结束位置
	prefixEnd := min(threshold, max(sqlPrefixLen, threshold-sqlSuffixLen))
	// 截取sql最后字符串的起始位置
	suffixStart := sqlLen - max(0, min(sqlSuffixLen, threshold-sqlPrefixLen))
	return prefixEnd, suffixStart, true
}

func fetchFullExecuteSQL(executeStmt *ast.ExecuteStmt, sessVars *variable.SessionVars) (origSQL string) {
	var (
		prepStmt *core.PlanCacheStmt
		err      error
	)
	if prepStmt, err = core.GetPreparedStmt(executeStmt, sessVars); err != nil {
		panic(err)
	}
	origSQL = prepStmt.PreparedAst.Stmt.OriginalText()

	var sb strings.Builder
	if executeStmt.Name != "" {
		sb.WriteString(";Type=SQL;Name=" + executeStmt.Name)
	} else {
		sb.WriteString(";Type=Binary")
	}
	sb.WriteString(";Params=[")
	for i, p := range prepStmt.PreparedAst.Params {
		if i > 0 {
			sb.WriteByte(',')
		}
		expr := ast.NewValueExpr(p.GetValue(), "", "")
		if err := expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
			panic(err)
		}
	}
	sb.WriteByte(']')
	if sb.Len() > 0 {
		origSQL += sb.String()
	}
	return origSQL
}

func render(sctx sessionctx.Context, traceLog *logSQLTraceObject) {
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
					traceLog.at = t
					traceLog.times.tidb, _ = time.ParseDuration("11s201ms")
					traceLog.times.all, _ = time.ParseDuration("3.315821ms")
					traceLog.times.parse, _ = time.ParseDuration("176.943µs")
					traceLog.times.plan, _ = time.ParseDuration("1.417613ms")
					traceLog.times.cop = copTimeObject{}
					traceLog.times.cop.wall, _ = time.ParseDuration("0s128ms")
					traceLog.times.cop.tikv, _ = time.ParseDuration("0s98ms")
					traceLog.times.cop.tiflash, _ = time.ParseDuration("12µs")
					traceLog.times.send, _ = time.ParseDuration("1ms")
					if traceLog.times.tx != nil {
						traceLog.times.tx.commit, _ = time.ParseDuration("100ms")
						traceLog.times.tx.prewrite, _ = time.ParseDuration("1s32ms")
					}
				}
			case "maxAct":
				traceLog.maxAct = int64(v.(float64))
			case "maxCop":
				traceLog.maxCop = &logMaxCopObject{}
				sub := v.(map[string]any)
				for ck, cv := range sub {
					switch ck {
					case "procAddr":
						traceLog.maxCop.procAddr = cv.(string)
					case "procTime":
						traceLog.maxCop.procTime, _ = time.ParseDuration(cv.(string))
					case "tasks":
						traceLog.maxCop.tasks = int(cv.(float64))
					}
				}
			case "rru":
				traceLog.ru.rru = v.(float64)
			case "wru":
				traceLog.ru.wru = v.(float64)
			case "mem":
				traceLog.mem = int64(v.(float64))
			case "disk":
				traceLog.disk = int64(v.(float64))
			case "rows":
				traceLog.rows = int64(v.(float64))
			case "tx":
				traceLog.tx = &logTXObject{}
				sub := v.(map[string]any)
				for ck, cv := range sub {
					switch ck {
					case "keys":
						traceLog.tx.keys = int(cv.(float64))
					case "affected":
						traceLog.tx.affected = uint64(cv.(float64))
					case "size":
						traceLog.tx.size = int(cv.(float64))
					}
				}
			case "err":
				traceLog.err = errors.New(v.(string))
			}
		}
	})

	renderTraceLog(sctx, traceLog)
}
