package mctech

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/stretchr/testify/require"
)

type getFileNameCase struct {
	input  string
	output string
	errMsg string
}

func TestLogFile(t *testing.T) {
	cases := []*getFileNameCase{
		// {"a/b/c/large-sql.log", "a/b/c/large-sql.log", ""},
		// {"a/b/c/{hostname}/large-sql.log", "a/b/c/tidb01/large-sql.log", ""},
		{"a/b/c/{hostname1}/large-sql.log", "a/b/c/tidb01/large-sql.log", "metrics log filename template DO NOT support 'hostname1' only allow 'hostname'"},
	}

	failpoint.Enable("github.com/pingcap/tidb/mctech/GetHostName", mock.M(t, "true"))

	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/mctech/GetHostName")
	}()

	for _, c := range cases {
		fn, err := getRealLogFile(c.input)
		if c.errMsg == "" {
			require.NoError(t, err)
			require.Equal(t, c.output, fn)
		} else {
			require.Error(t, err, c.errMsg)
		}
	}
}
