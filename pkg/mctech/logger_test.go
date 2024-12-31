package mctech

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/stretchr/testify/require"
)

type getFileNameCase struct {
	input  string
	output string
	errMsg string
}

func TestLogFile(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/GetHostName", mock.M(t, "true"))

	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/GetHostName")
	}()
	cases := []*getFileNameCase{
		// {"a/b/c/large-sql.log", "a/b/c/large-sql.log", ""},
		// {"a/b/c/{hostname}/large-sql.log", "a/b/c/tidb01/large-sql.log", ""},
		{"a/b/c/{hostname1}/large-sql.log", "a/b/c/tidb01/large-sql.log", "metrics log filename template DO NOT support 'hostname1' only allow 'hostname'"},
	}

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
