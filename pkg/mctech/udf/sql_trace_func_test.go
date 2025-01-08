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
	_, _, err := GetFullSQL(types.MinTimestamp, 1234567890, "")
	require.ErrorContains(t, err, "未设置 mctech.metrics.sql-trace.full-sql-dir 配置项")
}

func TestGetFullSql(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594437)), mysql.TypeDatetime, 3)
	sql, isNull, err := GetFullSQL(datetime, 1697003594435, "")
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 279828, len(sql))
}

func TestGetFullSqlWithPre(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594437)), mysql.TypeDatetime, 3)
	sql, isNull, err := GetFullSQL(datetime, 1697003594436, "pre")
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 279828, len(sql))
}

func TestGetFullSqlWithProduct(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath, "Metrics.SqlTrace.Group": "product"}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594437)), mysql.TypeDatetime, 3)
	sql, isNull, err := GetFullSQL(datetime, 1697003594437, "")
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, 279828, len(sql))
}

func TestGetFullSqlNotExists(t *testing.T) {
	fullPath, err := filepath.Abs("./data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594499)), mysql.TypeDatetime, 3)
	_, _, err = GetFullSQL(datetime, 1697003594437, "product")
	require.NoError(t, err)
}
