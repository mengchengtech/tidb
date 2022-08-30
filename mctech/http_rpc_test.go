package mctech

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHttpRpcReturnError(t *testing.T) {
	get, err := http.NewRequest("GET", "http://localhost", nil)
	require.NoError(t, err)

	_, err = DoRequest(get)
	require.ErrorContainsf(t, err, `Get "http://localhost": dial tcp`, "")
}
