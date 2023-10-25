package udf

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestGetFullSqlNotSetConfig(t *testing.T) {
	_, err := GetFullSql("tidb05", "accccc", types.MinTimestamp)
	require.ErrorContains(t, err, "未设置 mctech_metrics_sql_trace_full_sql_dir 全局变量的值")
}

func TestGetFullSql(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594436)), mysql.TypeDatetime, 3)
	sql, err := GetFullSql("tidb05", "5qz4J4Ux23z", datetime)
	require.NoError(t, err)
	require.Equal(t, 279828, len(sql))
	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}
