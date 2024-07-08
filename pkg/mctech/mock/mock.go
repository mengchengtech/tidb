//go:build !intest

package mock

import (
	"errors"

	"github.com/stretchr/testify/require"
)

func M(t require.TestingT, v interface{}) string {
	panic(errors.New("SHOULD NOT call out of test"))
}
