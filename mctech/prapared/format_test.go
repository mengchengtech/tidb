package prapared

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/stretchr/testify/require"
)

type converterTestCase struct {
	name    string
	value   string
	expect  any
	failure string
}

func (c *converterTestCase) Failure() string {
	return c.failure
}

func (c *converterTestCase) Source() any {
	return fmt.Sprintf("%s -> %s", c.name, c.value)
}

func createGlobalInfo(global bool, excludes []string) *mctech.GlobalValueInfo {
	return &mctech.GlobalValueInfo{
		Global:   global,
		Excludes: excludes,
	}
}

func TestValueConverter(t *testing.T) {
	cases := []*converterTestCase{
		{"global", "", "", "global的值错误。可选值为'true', 'false', '1', '0'"},
		{"global", "1", createGlobalInfo(true, nil), ""},
		{"global", "0", createGlobalInfo(false, nil), ""},
		{"GLOBAL", "false", createGlobalInfo(false, nil), ""},
		{"GLOBAL", "true", createGlobalInfo(true, nil), ""},
		{"GLObal", "TRUE", createGlobalInfo(true, nil), ""},
		{"GLOBAL", "FALSE", createGlobalInfo(false, nil), ""},
		{"GLOBAL", "false", createGlobalInfo(false, nil), ""},
		{"global", "!ys", createGlobalInfo(true, []string{"ys"}), ""},
		{"global", "!ys, !mctech", createGlobalInfo(true, []string{"ys", "mctech"}), ""},
		{"dbPrefix", "abc", "", "dbPrefix的值错误。可选值为'true', 'false', '1', '0'"},
	}

	doRunTest(t, convertRunTestCase, cases)
}

func convertRunTestCase(t *testing.T, c *converterTestCase) error {
	gc := NewGlobalValueFormatter()
	out, err := gc.Format(c.name, c.value)
	if err != nil {
		return err
	}
	require.Equal(t, c.expect, out, c.Source())
	return nil
}
