//go:build !intest

package mctech

import (
	"errors"
	"testing"
)

func TestMarshal(t *testing.T, v interface{}) string {
	panic(errors.New("SHOULD NOT call out of test"))
}
