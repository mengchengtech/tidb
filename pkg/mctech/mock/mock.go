// 不在测试环境不会编译当前代码

//go:build !intest

package mock

import (
	"errors"

	"github.com/stretchr/testify/require"
)

func M(t require.TestingT, v any) string {
	panic(errors.New("SHOULD NOT call out of test"))
}
