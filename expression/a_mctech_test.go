// add by zhangbing

package expression

import (
	"testing"

	"github.com/pingcap/tidb/parser/ast"
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
