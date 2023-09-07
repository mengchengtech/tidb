// add by zhangbing

package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type strToSliceCase struct {
	source         string
	sep            string
	expect         []string
	possibleValues []string
	result         bool
}

// StrToSlice
func TestStrToSlice(t *testing.T) {
	cases := []*strToSliceCase{
		{source: "     ", sep: ",", expect: []string{}},
		{source: " ,", sep: ",", expect: []string{}},
		{source: " ,,,,,", sep: ",", expect: []string{}},
		{source: "", sep: ",", expect: []string{}},
		{source: " aa ,c,, ee, f ,aa,", sep: ",", expect: []string{"aa", "c", "ee", "f"}},
		{source: " aa |c|| ee| f ,aa,", sep: "|", expect: []string{"aa", "c", "ee", "f ,aa,"}},
		{source: " aa,ee,f a,", sep: ",", expect: []string{"aa", "ee", "f a"}},
	}

	for _, c := range cases {
		list := StrToSlice(c.source, c.sep)
		require.ElementsMatch(t, list, c.expect, fmt.Sprint(c.source))
	}
}

// StrToPossibleValueSlice
func TestStrToPossibleValueSlice(t *testing.T) {
	cases := []*strToSliceCase{
		{source: "     ", sep: ",", expect: []string{}, result: true, possibleValues: []string{"aa", "ee"}},
		{source: " ,", sep: ",", expect: []string{}, result: true, possibleValues: []string{"aa", "ee"}},
		{source: " ,,,,,", sep: ",", expect: []string{}, result: true, possibleValues: []string{"aa", "ee"}},
		{source: "", sep: ",", expect: []string{}, result: true, possibleValues: []string{"aa", "ee"}},
		{source: " aa ,c,, ee, f ,aa,", sep: ",", result: false, possibleValues: []string{"aa", "ee"}},
		{source: " aa | || ee ", sep: "|", expect: []string{"aa", "ee"}, result: true, possibleValues: []string{"aa", "ee"}},
		{source: " aa", sep: ",", expect: []string{"aa"}, result: true, possibleValues: []string{"aa", "ee"}},
	}

	for _, c := range cases {
		list, _, ok := StrToPossibleValueSlice(c.source, c.sep, c.possibleValues)
		require.Equal(t, c.result, ok, fmt.Sprintf("input: [%s]", c.source))
		if ok {
			require.ElementsMatch(t, list, c.expect, fmt.Sprintf("input: [%s]", c.source))
		}
	}
}
