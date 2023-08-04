package mock

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/util/intest"
	"github.com/stretchr/testify/require"
)

func M(t *testing.T, v interface{}) string {
	if !intest.InTest {
		panic(errors.New("SHOULD NOT call out of test"))
	}

	if s, ok := v.(string); ok {
		return fmt.Sprintf("return(`%s`)", s)
	}

	bytes, err := json.Marshal(v)
	require.NoError(t, err)
	return fmt.Sprintf("return(`%s`)", string(bytes))
}
