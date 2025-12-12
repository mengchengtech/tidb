// add by zhangbing

package expression

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech"
	mmock "github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestMCTechSequence(t *testing.T) {
	ctx := createContext(t)

	for _, name := range []string{ast.MCTechSequence, ast.MCSeq} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(nil))
		require.NoError(t, err)
		resetStmtContext(ctx)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		n := v.GetInt64()
		require.Greater(t, n, int64(0))
	}
}

func TestMCTechSequenceDecode(t *testing.T) {
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechSequenceDecode, ast.MCSeqDecode} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(1318030351881216)))
		require.NoError(t, err)
		resetStmtContext(ctx)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		n := v.GetMysqlTime()
		require.NoError(t, err)
		require.Equal(t, "2022-07-18 10:59:52", n.String())
	}
}

func TestMCTechJustVersionPass(t *testing.T) {
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechVersionJustPass, ast.MCVersionJustPass} {
		fc := funcs[name]
		argsCases := [][]any{{}, {30}, {-50}}
		for _, args := range argsCases {
			f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
			require.NoError(t, err)
			resetStmtContext(ctx)
			v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
			require.NoError(t, err)
			n := v.GetInt64()
			require.Greater(t, n, int64(0))
		}
	}
}

func TestMCTechEncrypt(t *testing.T) {
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechEncrypt, ast.MCEncrypt} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums("bindsang")))
		require.NoError(t, err)
		resetStmtContext(ctx)
		_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
	}
}
func TestMCTechDecrypt(t *testing.T) {
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechDecrypt, ast.MCDecrypt} {
		fc := funcs[name]

		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums("{crypto}a4UzL7Cnyyc+D/sK6U7GJA==")))
		require.NoError(t, err)
		resetStmtContext(ctx)
		_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
	}
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mmock.M(t, map[string]any{
			"Crypto.CRYPTO": map[string]any{"type": "aes", "key": "W1gfHNQTARa7Uxt7wua8Aw==", "iv": "a9Z5R6YCjYx1QmoG5WF9BQ=="},
		}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/udf/GetCryptoClient", mmock.M(t, "true"))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")
		failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/udf/GetCryptoClient")
	}()

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
		for _, name := range []string{ast.MCTechDecrypt, ast.MCDecrypt} {
			fc := funcs[name]

			f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
			require.NoError(t, err)
			resetStmtContext(ctx)
			result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
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
}

func TestGetFullSqlWithNotConfig(t *testing.T) {
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechGetFullSql, ast.MCGetFullSql} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx,
			datumsToConstants(types.MakeDatums("2023-10-10 19:55:40", 1697003594436)))
		require.NoError(t, err)
		resetStmtContext(ctx)
		_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		require.ErrorContains(t, err, "未设置 mctech.metrics.sql-trace.full-sql-dir 配置项")
	}
}

func TestGetFullSqlByTime(t *testing.T) {
	fullPath, err := filepath.Abs("../mctech/udf/data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mmock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := "2023-10-11 13:53:14.437"
	at, err := time.ParseInLocation("2006-01-02 15:04:05.999", datetime, time.Local)
	require.NoError(t, err)
	unixMilli := at.UnixMilli()
	require.Equal(t, int64(1697003594437), unixMilli)
	dt := types.NewTime(types.FromGoTime(at), mysql.TypeDatetime, 3)
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechGetFullSql, ast.MCGetFullSql} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(dt, 1697003594435)))
		require.NoError(t, err)
		resetStmtContext(ctx)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.False(t, d.IsNull())
	}
}

func TestGetFullSqlByString(t *testing.T) {
	fullPath, err := filepath.Abs("../mctech/udf/data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mmock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := "2023-10-11 13:53:14.437"
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechGetFullSql, ast.MCGetFullSql} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(datetime, "1697003594436", "pre")))
		require.NoError(t, err)
		resetStmtContext(ctx)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.False(t, d.IsNull())
	}
}

func TestGetFullSqlByStringAndGroup(t *testing.T) {
	fullPath, err := filepath.Abs("../mctech/udf/data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mmock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath, "Metrics.SqlTrace.Group": "product"}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := "2023-10-11 13:53:14.437"
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechGetFullSql, ast.MCGetFullSql} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(datetime, "1697003594437")))
		require.NoError(t, err)
		resetStmtContext(ctx)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.False(t, d.IsNull())
	}
}

func TestGetFullSqlNotExists(t *testing.T) {
	fullPath, err := filepath.Abs("../mctech/udf/data")
	require.NoError(t, err)
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mmock.M(t, map[string]string{"Metrics.SqlTrace.FullSqlDir": fullPath}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	datetime := "2099-10-11 13:53:14.437"
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechGetFullSql, ast.MCGetFullSql} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(datetime, 1697003594437, "product")))
		require.NoError(t, err)
		resetStmtContext(ctx)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.True(t, d.IsNull())
	}
}

func TestDataWarehouseIndexInfo(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mmock.M(t, map[string]any{
			"DWIndex.Current": map[string]any{"current": 1},
		}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")
	}()

	ctx := createContext(t)
	for _, name := range []string{ast.MCTechDataWarehouseIndexInfo, ast.MCDWIndexInfo} {
		fc := funcs[name]
		f, err := fc.getFunction(ctx, datumsToConstants(nil))
		require.NoError(t, err)
		resetStmtContext(ctx)
		mctx, err := mctech.WithNewContext(ctx.Sctx())
		require.NoError(t, err)
		roles := testFlagRoles(0)
		result, err := mctech.NewParseResult("", &roles, nil, map[string]any{})
		require.NoError(t, err)
		modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
		modifyCtx.SetParseResult(result)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		data, err := d.GetMysqlJSON().MarshalJSON()
		require.NoError(t, err)
		info := mctech.DWIndexInfo{}
		err = json.Unmarshal(data, &info)
		require.NoError(t, err)
		require.Equal(t, mctech.DWIndexInfo{Current: 1, Background: 2}, info)
	}
}

func TestMCTechHelp(t *testing.T) {
	ctx := createContext(t)
	for _, name := range []string{ast.MCTechHelp, ast.MCHelp} {
		for _, showHidden := range []bool{false, true} {
			fc := funcs[name]
			var args []types.Datum
			if showHidden {
				args = types.MakeDatums(true)
			}
			f, err := fc.getFunction(ctx, datumsToConstants(args))
			require.NoError(t, err)
			resetStmtContext(ctx)
			d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
			require.NoError(t, err)
			content := d.GetString()
			fmt.Print(content)
		}
	}
}

type testFlagRoles int

func (*testFlagRoles) TenantOmit() bool {
	return true
}

func (*testFlagRoles) TenantOnly() bool {
	return false
}

func (*testFlagRoles) AcrossDB() bool {
	return false
}
