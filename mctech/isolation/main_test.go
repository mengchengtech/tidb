package isolation

import (
	"testing"

	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/stretchr/testify/require"
)

var dbMap = map[string]string{
	"pf": "global_platform",
	"dw": "global_dw",
}

type mctechTestCase interface {
	Source() any
	Expect() string
	Failure() string
}

type runTestCaseType[T mctechTestCase] func(t *testing.T, tbl T) error

func doRunTest[T mctechTestCase](t *testing.T, runTestCase runTestCaseType[T], cases []T) {
	for _, c := range cases {
		err := runTestCase(t, c)
		failure := c.Failure()
		if err == nil && failure == "" {
			continue
		}

		if failure != "" {
			require.ErrorContainsf(t, err, failure, "source %v", c.Source())
		} else {
			require.NoErrorf(t, err, "source %v", c.Source())
		}
	}
}
