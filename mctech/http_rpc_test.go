package mctech

import (
	"net/http"
	"testing"

	"github.com/pingcap/failpoint"
	mmock "github.com/pingcap/tidb/mctech/mock"
	"github.com/stretchr/testify/require"
)

func TestHttpRpcReturnError(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/mctech/MockMctechHttp", mmock.M(t, map[string]any{}))
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/MockMctechHttp")

	get, err := http.NewRequest("GET", "http://localhost/rpc-test", nil)
	require.NoError(t, err)

	_, _, err = DoRequest(get)
	require.ErrorContainsf(t, err, "rpc调用发生错误。详情请查询tidb服务日志", "")
}
