//go:build intest

package mock

import (
	"encoding/json"
	"fmt"

	"github.com/stretchr/testify/require"
)

func M(t require.TestingT, v any) string {
	bytes, err := json.Marshal(v)
	require.NoError(t, err)
	return fmt.Sprintf("return(`%s`)", string(bytes))
}
