// 不在测试环境不会编译当前代码

//go:build !intest

package mock

import (
	"errors"
	"testing"
)

func M(t *testing.T, v interface{}) string {
	panic(errors.New("SHOULD NOT call out of test"))
}
