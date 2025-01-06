package variable

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testAtomcCase struct {
	IntValue  int
	StrValue  string
	BoolValue bool
	StrArray  []string
}

func TestAtomicInt(t *testing.T) {
	c := testAtomcCase{IntValue: 1}
	b := atomicLoad(&c.IntValue)
	require.Equal(t, 1, b)
	atomicStore(&c.IntValue, 100)
	require.Equal(t, 100, c.IntValue)
	b = atomicLoad(&c.IntValue)
	require.Equal(t, 100, b)
}

func TestAtomicBool(t *testing.T) {
	c := testAtomcCase{BoolValue: true}
	b := atomicLoad(&c.BoolValue)
	require.Equal(t, true, b)
	atomicStore(&c.BoolValue, false)
	require.Equal(t, false, c.BoolValue)
	b = atomicLoad(&c.BoolValue)
	require.Equal(t, false, b)
}

func TestAtomicString(t *testing.T) {
	c := testAtomcCase{StrValue: "noname"}
	str := atomicLoad(&c.StrValue)
	require.Equal(t, "noname", str)
	atomicStore(&c.StrValue, "new name")
	require.Equal(t, "new name", c.StrValue)
	str = atomicLoad(&c.StrValue)
	require.Equal(t, "new name", str)
}

func TestAtomicStrings(t *testing.T) {
	c := testAtomcCase{StrArray: []string{"a", "b"}}
	arr := atomicLoad(&c.StrArray)
	require.Equal(t, []string{"a", "b"}, arr)
	atomicStore(&c.StrArray, []string{"c"})
	require.Equal(t, []string{"c"}, c.StrArray)
	arr = atomicLoad(&c.StrArray)
	require.Equal(t, []string{"c"}, arr)
}
