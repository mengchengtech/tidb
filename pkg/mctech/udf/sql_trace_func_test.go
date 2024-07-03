package udf

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetFullSqlNotSetConfig(t *testing.T) {
	_, err := GetFullSQL("tidb05", "accccc", types.MinTimestamp)
	require.ErrorContains(t, err, "未设置 mctech_metrics_sql_trace_full_sql_dir 全局变量的值")
}

func TestGetFullSql(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594436)), mysql.TypeDatetime, 3)
	sql, err := GetFullSQL("tidb05", "5qz4J4Ux23z", datetime)
	require.NoError(t, err)
	require.Equal(t, 279828, len(sql))
}
