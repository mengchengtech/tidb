//go:build intest

package interceptor

import (
	"encoding/json"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/zap/zapcore"
)

type traceFullQueryKeyType int

func (k traceFullQueryKeyType) String() string {
	return "trace_full_query"
}

// 单元测试代码使用
const traceFullQueryKey traceFullQueryKeyType = 0

func renderTraceLog(sctx sessionctx.Context, fields []zapcore.Field) {
	cc := zapcore.EncoderConfig{
		TimeKey:        "", // 不记录生成日志时的time
		LevelKey:       "", // 不记录日志级别
		NameKey:        "", // 不记录日志对象的名称
		CallerKey:      "", // 不记录日志所在方法调用者信息
		MessageKey:     "", // 不记录日志的"message"内容
		StacktraceKey:  "", // 不记录日志调用时的调用堆栈信息
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     log.DefaultTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   log.ShortCallerEncoder,
	}
	encoder := zapcore.NewJSONEncoder(cc)
	ent := zapcore.Entry{
		LoggerName: "trace.ut",
		Time:       time.Now(),
		Level:      zapcore.InfoLevel,
		Message:    "",
	}
	if buf, err := encoder.EncodeEntry(ent, fields); err != nil {
		panic(err)
	} else {
		sctx.SetValue(traceFullQueryKey, buf.Bytes())
	}
}

// GetFullQueryTraceLog 获取运行单元测试时，在上下文中填入的日志对象
func GetFullQueryTraceLog(sctx sessionctx.Context) (map[string]any, error) {
	if data, ok := sctx.Value(traceFullQueryKey).([]byte); ok {
		var logData map[string]any
		if err := json.Unmarshal(data, &logData); err != nil {
			return nil, err
		}
		return logData, nil
	}
	return nil, nil
}

// EncodeForTest 仅用于单元测试
func EncodeForTest(num uint64) string {
	return encode(num)
}
