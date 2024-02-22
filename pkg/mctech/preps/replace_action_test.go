package preps

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type replaceTestCase struct {
	format  string
	tenant  string
	params  map[string]any
	failure string
}

func (c *replaceTestCase) Failure() string {
	if strings.Contains(c.failure, "%") {
		return fmt.Sprintf(c.failure, c.format)
	}
	return c.failure
}

func (c *replaceTestCase) Source() any {
	return fmt.Sprintf("%s -> %s", c.format, c.tenant)
}

func TestReplaceAction(t *testing.T) {
	cases := []*replaceTestCase{
		{"tenant='%s'", "gslq", nil, ""},
		{"tenant=%s", "ztsj", nil, ""},
		{"tenant", "", map[string]any{}, "执行[replace]时未找到名称为'%s'的参数的值"},
		{"tenant", "ztsj", map[string]any{"tenant": "ztsj"}, ""},
	}
	doRunTest(t, testReplaceCase, cases)
}

func testReplaceCase(t *testing.T, c *replaceTestCase) error {
	sql := "select * from {{tenant}}_platform.t1 a inner join {{tenant}}_platform.t2 on t1.id = t2.id"
	var args = c.format
	if strings.Contains(c.format, "%") {
		args = fmt.Sprintf(c.format, c.tenant)
	}
	action := &replaceAction{}
	outSQL, err := action.Resolve(sql, args, c.params)

	if err != nil {
		return err
	}

	require.NotContains(t, outSQL, "{{tenant}}")
	require.Contains(t, outSQL, fmt.Sprintf("%s_", c.tenant), c.Source())
	return nil
}
