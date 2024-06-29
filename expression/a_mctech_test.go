// add by zhangbing

package expression

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	mmock "github.com/pingcap/tidb/mctech/mock"
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

type decryptCase struct {
	cipher     string
	maskFrom   any
	maskLength any
	maskChar   string
	expected   any
	errMsg     string
}

func (c *decryptCase) String() string {
	return fmt.Sprintf("cipher:%s,maskFrom:%v,maskLength:%v", c.cipher, c.maskFrom, c.maskLength)
}

func TestMCTechDecryptMask(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/MockMctechHttp",
		mmock.M(t, map[string]string{"key": "W1gfHNQTARa7Uxt7wua8Aw==", "iv": "a9Z5R6YCjYx1QmoG5WF9BQ=="}),
	)
	failpoint.Enable("github.com/pingcap/tidb/mctech/udf/GetCryptoClient", mmock.M(t, "true"))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/mctech/MockMctechHttp")
		failpoint.Disable("github.com/pingcap/tidb/mctech/udf/GetCryptoClient")
	}()

	fc := funcs[ast.MCTechDecrypt]

	ctx := createContext(t)
	// "13511868785"
	cipher := "{crypto}HMvlbGus4V3geqwFULvOUw=="
	cases := []decryptCase{
		{cipher, nil, 0, "", nil, ""},
		{cipher, 0, nil, "", nil, ""},
		{cipher, 0, 0, "", nil, "'maskFrom' (0) out of range [1, +inf]"},
		{cipher, 0, 4, "", nil, "'maskFrom' (0) out of range [1, +inf]"},
		{cipher, 1, 0, "", nil, "'maskLength' (0) out of range [1, 127]"},
		{cipher, -1, 0, "", nil, "'maskFrom' (-1) out of range [1, +inf]"},
		{cipher, 1, -1, "", nil, "'maskLength' (-1) out of range [1, 127]"},
		{cipher, 1, 128, "", nil, "'maskLength' (128) out of range [1, 127]"},
		{cipher, 1, 127, "", "***********", ""},
		{cipher, 1, 11, "", "***********", ""},
		{cipher, 1, 10, "", "**********5", ""},
		{cipher, 1, 1, "", "*3511868785", ""},
		{cipher, 11, 127, "", "1351186878*", ""},
		{cipher, 12, 127, "", "13511868785", ""},
		{cipher, 15, 127, "", "13511868785", ""},
		{cipher, 4, 4, "", "135****8785", ""},
		{cipher, 1, 4, "", "****1868785", ""},

		{cipher, 1, 127, "@", "@@@@@@@@@@@", ""},
		{cipher, 1, 11, "@", "@@@@@@@@@@@", ""},
		{cipher, 1, 10, "@", "@@@@@@@@@@5", ""},
		{cipher, 1, 1, "@", "@3511868785", ""},
		{cipher, 11, 127, "@", "1351186878@", ""},
		{cipher, 12, 127, "@", "13511868785", ""},
		{cipher, 15, 127, "@", "13511868785", ""},
		{cipher, 4, 4, "@", "135@@@@8785", ""},
		{cipher, 1, 4, "@", "@@@@1868785", ""},

		{cipher, 1, 127, "##", "######################", ""},
		{cipher, 1, 11, "##", "######################", ""},
		{cipher, 1, 10, "##", "####################5", ""},
		{cipher, 1, 1, "##", "##3511868785", ""},
		{cipher, 11, 127, "##", "1351186878##", ""},
		{cipher, 12, 127, "##", "13511868785", ""},
		{cipher, 15, 127, "##", "13511868785", ""},
		{cipher, 4, 4, "##", "135########8785", ""},
		{cipher, 1, 4, "##", "########1868785", ""},
	}

	for _, c := range cases {
		args := []any{c.cipher, c.maskFrom, c.maskLength}
		if len(c.maskChar) > 0 {
			args = append(args, c.maskChar)
		}
		f, err := fc.getFunction(mock.NewContext(), datumsToConstants(types.MakeDatums(args...)))
		require.NoError(t, err)
		resetStmtContext(ctx)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		if len(c.errMsg) > 0 {
			require.Error(t, err, c.String())
			require.Equal(t, c.errMsg, err.Error(), c.String())
		} else {
			require.NoError(t, err, c.String())
			value := result.GetValue()
			require.Equal(t, c.expected, value, c.String())
		}
	}
}

func TestGetFullSqlWithNotConfig(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCGetFullSql]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("2023-10-10 19:55:40", 1697003594436)))
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
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")

	datetime := "2023-10-11 13:53:14.436"
	at, err := time.ParseInLocation("2006-01-02 15:04:05.999", datetime, time.Local)
	require.NoError(t, err)
	unixMilli := at.UnixMilli()
	require.Equal(t, int64(1697003594436), unixMilli)
	dt := types.NewTime(types.FromGoTime(at), mysql.TypeDatetime, 3)
	ctx := createContext(t)
	fc := funcs[ast.MCGetFullSql]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums(dt, 1697003594436)))
	require.NoError(t, err)
	resetStmtContext(ctx)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
}

func TestGetFullSqlByString(t *testing.T) {
	fullPath, err := filepath.Abs("../mctech/udf/data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		fmt.Sprintf("return(`{\"Metrics.SqlTrace.FullSqlDir\": \"%s\"}`)", fullPath),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")

	datetime := "2023-10-11 13:53:14.436"
	ctx := createContext(t)
	fc := funcs[ast.MCGetFullSql]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums(datetime, "1697003594436")))
	require.NoError(t, err)
	resetStmtContext(ctx)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
}
