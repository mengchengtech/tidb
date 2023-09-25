// 测试环境编译当前代码，配合failpoint模块使用

//go:build intest

package mock

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func M(t *testing.T, v interface{}) string {
	if s, ok := v.(string); ok {
		return fmt.Sprintf("return(`%s`)", s)
	}

	bytes, err := json.Marshal(v)
	require.NoError(t, err)
	return fmt.Sprintf("return(`%s`)", string(bytes))
}
