// add by zhangbing

package expression

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestMCTechSequence(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechSequence]
	f, err := fc.getFunction(mock.NewContext(), datumsToConstants(nil))
	require.NoError(t, err)
	resetStmtContext(ctx)
	v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	n := v.GetInt64()
	require.Greater(t, n, int64(0))

	waitForBackendRelease()
}

func TestMCTechJustVersionPass(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechVersionJustPass]
	f, err := fc.getFunction(mock.NewContext(), datumsToConstants(nil))
	require.NoError(t, err)
	resetStmtContext(ctx)
	v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	n := v.GetInt64()
	require.Greater(t, n, int64(0))
	waitForBackendRelease()
}

func TestMCTechEncrypt(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechEncrypt]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("bindsang")))
	require.NoError(t, err)
	resetStmtContext(ctx)
	v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	n := v.GetString()
	require.Equal(t, "{crypto}a4UzL7Cnyyc+D/sK6U7GJA==", n)
	waitForBackendRelease()
}

func TestMCTechDecrypt(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.MCTechDecrypt]
	f, err := fc.getFunction(mock.NewContext(),
		datumsToConstants(types.MakeDatums("{crypto}a4UzL7Cnyyc+D/sK6U7GJA==")))
	require.NoError(t, err)
	resetStmtContext(ctx)
	v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	n := v.GetString()
	require.Equal(t, "bindsang", n)
	waitForBackendRelease()
}

func waitForBackendRelease() {
	time.Sleep(1 * time.Second)
}
