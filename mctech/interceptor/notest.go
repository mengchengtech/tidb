//go:build !intest

package interceptor

import (
	"errors"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/sessionctx"
	"go.uber.org/zap"
)

func renderTraceLog(_ sessionctx.Context, traceLog *logSQLTraceObject) {
	mctech.F().Info(
		"", // 忽略Message字段
		zap.Inline(traceLog),
	)
}

// GetFullQueryTraceLog placeholder. not allow invoke
func GetFullQueryTraceLog(sctx sessionctx.Context) (map[string]any, error) {
	return nil, errors.New("[GetFullQueryTraceLog] not allow invoke")
}

// EncodeForTest placeholder. not allow invoke
func EncodeForTest(num uint64) string {
	err := errors.New("[EncodeForTest] not allow invoke")
	panic(err)
}
