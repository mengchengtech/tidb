package interceptor

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx"
)

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
