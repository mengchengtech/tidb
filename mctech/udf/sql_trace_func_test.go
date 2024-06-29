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
	_, _, err := GetFullSQL(types.MinTimestamp, 1234567890)
	require.ErrorContains(t, err, "未设置 mctech_metrics_sql_trace_full_sql_dir 全局变量的值")
}

func TestGetFullSql(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594436)), mysql.TypeDatetime, 3)
	sql, isNull, err := GetFullSQL(datetime, 1697003594436)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 279828, len(sql))
}

func TestGetFullSql2(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594437)), mysql.TypeDatetime, 3)
	sql, isNull, err := GetFullSQL(datetime, 1697003594435)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 279828, len(sql))
}

func TestGetFullSqlNotExists(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594436)), mysql.TypeDatetime, 3)
	_, isNull, err := GetFullSQL(datetime, 1697003594437)
	require.NoError(t, err)
	require.True(t, isNull)
}
