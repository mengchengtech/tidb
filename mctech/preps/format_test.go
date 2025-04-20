package preps_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/preps"
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

func (c *converterTestCase) Source(i int) any {
	return fmt.Sprintf("(%d) %s -> %s", i, c.name, c.value)
}

func TestValueConverter(t *testing.T) {
	cases := []*converterTestCase{
		{"global", "", "", "global的值错误。可选值为'true', 'false', '1', '0'"},
		{"global", "1", mctech.NewGlobalValue(true, nil, nil), ""},
		{"global", "0", mctech.NewGlobalValue(false, nil, nil), ""},
		{"GLOBAL", "false", mctech.NewGlobalValue(false, nil, nil), ""},
		{"GLOBAL", "true", mctech.NewGlobalValue(true, nil, nil), ""},
		{"GLObal", "TRUE", mctech.NewGlobalValue(true, nil, nil), ""},
		{"GLOBAL", "FALSE", mctech.NewGlobalValue(false, nil, nil), ""},
		{"GLOBAL", "false", mctech.NewGlobalValue(false, nil, nil), ""},
		{"global", "!ys", mctech.NewGlobalValue(true, []string{"ys"}, nil), ""},
		{"global", "!ys, !mctech", mctech.NewGlobalValue(true, []string{"ys", "mctech"}, nil), ""},
		{"global", "!ys,-mctech", mctech.NewGlobalValue(true, []string{"ys", "mctech"}, nil), ""},
		{"global", "!ys,-mctech,+gslq", mctech.NewGlobalValue(true, []string{"ys", "mctech"}, []string{"gslq"}), ""},
		{"global", "+gslq,+mctech", mctech.NewGlobalValue(true, nil, []string{"gslq", "mctech"}), ""},
		{"dbPrefix", "abc", "", "dbPrefix的值错误。可选值为'true', 'false', '1', '0'"},
	}

	doRunTest(t, convertRunTestCase, cases)
}

func convertRunTestCase(t *testing.T, i int, c *converterTestCase) error {
	gc := preps.NewGlobalValueFormatterForTest()
	out, err := gc.Format(c.name, c.value)
	if err != nil {
		return err
	}
	require.Equal(t, c.expect, out, c.Source(i))
	return nil
}
