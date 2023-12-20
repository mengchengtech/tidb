// add by zhangbing

package expression

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestMCTechSequence(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechSequence]
	f, err := fc.getFunction(mock.NewContext(), datumsToConstants(nil))
	require.NoError(t, err)
	resetStmtContext(ctx)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	n := v.GetInt64()
	require.Greater(t, n, int64(0))
}

func TestMCTechSequenceDecode(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechSequenceDecode]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums(1318030351881216)))
	require.NoError(t, err)
	resetStmtContext(ctx)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	n := v.GetMysqlTime()
	require.NoError(t, err)
	require.Equal(t, "2022-07-18 10:59:52", n.String())
}

func TestMCTechJustVersionPass(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechVersionJustPass]
	f, err := fc.getFunction(mock.NewContext(), datumsToConstants(nil))
	require.NoError(t, err)
	resetStmtContext(ctx)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	n := v.GetInt64()
	require.Greater(t, n, int64(0))
}

func TestMCTechEncrypt(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechEncrypt]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("bindsang")))
	require.NoError(t, err)
	resetStmtContext(ctx)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
}

func TestMCTechDecrypt(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechDecrypt]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("{crypto}a4UzL7Cnyyc+D/sK6U7GJA==")))
	require.NoError(t, err)
	resetStmtContext(ctx)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
}

func TestGetFullSqlWithNotConfig(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCGetFullSql]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("tidb06", "5MRDLP6DN2B", "2023-10-10 19:55:40")))
	require.NoError(t, err)
	resetStmtContext(ctx)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.ErrorContains(t, err, "未设置 mctech_metrics_sql_trace_full_sql_dir 全局变量的值")
}

func TestGetFullSqlByTime(t *testing.T) {
	fullPath, err := filepath.Abs("../mctech/udf/data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		fmt.Sprintf("return(`{\"Metrics.SqlTrace.FullSqlDir\": \"%s\"}`)", fullPath),
	)

	datetime := types.NewTime(types.FromGoTime(time.UnixMilli(1697003594436)), mysql.TypeDatetime, 3)
	ctx := createContext(t)
	fc := funcs[ast.MCGetFullSql]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("tidb05", "5qz4J4Ux23z", datetime)))
	require.NoError(t, err)
	resetStmtContext(ctx)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)

	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}

func TestGetFullSqlByString(t *testing.T) {
	fullPath, err := filepath.Abs("../mctech/udf/data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		fmt.Sprintf("return(`{\"Metrics.SqlTrace.FullSqlDir\": \"%s\"}`)", fullPath),
	)

	ctx := createContext(t)
	fc := funcs[ast.MCGetFullSql]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("tidb05", "5qz4J4Ux23z", "2023-10-11 05:53:14.436")))
	require.NoError(t, err)
	resetStmtContext(ctx)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)

	failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
}
