package preps_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/mctech/preps"
	"github.com/stretchr/testify/require"
)

type testContextCase struct {
	tenant   string
	response map[string]any
	expect   mctech.DWIndex
	params   map[string]any
	failure  string
}

func (c *testContextCase) Failure() string {
	return c.failure
}

func (c *testContextCase) Source(i int) any {
	return fmt.Sprintf("(%d) %s", i, c.response)
}

func TestDWSelectorGetDWIndex(t *testing.T) {
	cases := []*testContextCase{
		{"gslq", map[string]any{"DWIndex.Current": map[string]any{"current": ""}}, 1, map[string]any{"background": true}, "cannot unmarshal"},
		{"gslq", map[string]any{"DWIndex.ByRequest": map[string]any{"db": ""}}, 1, map[string]any{"requestId": "12345678"}, "cannot unmarshal"},
		{"gslq", map[string]any{"DWIndex.Current": map[string]any{"current": 2}}, 2, map[string]any{}, ""},
		{"gslq", map[string]any{"DWIndex.Current": map[string]any{"current": 2}}, 2, map[string]any{}, ""}, // 测试缓存中获取
		{"gslq", map[string]any{"DWIndex.Current": map[string]any{"current": 2}}, 1, map[string]any{"background": true}, ""},
		{"gslq", map[string]any{"DWIndex.ByRequest": map[string]any{"db": 1}}, 1, map[string]any{"requestId": "12345678"}, ""},
		{"gslq", map[string]any{"DWIndex.ByRequest": map[string]any{"db": 1}}, 1, map[string]any{"requestId": "12345678"}, ""}, // 测试重复执行缓存
	}

	doRunTest(t, contextRunTestCase, cases)
}

func contextRunTestCase(t *testing.T, i int, c *testContextCase) error {
	failpoint.Enable("github.com/pingcap/tidb/mctech/MockMctechHttp",
		mock.M(t, c.response),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/MockMctechHttp")

	roles, err := preps.NewFlagRoles(true, false, false)
	if err != nil {
		return err
	}
	result, err := mctech.NewPrepareResult(c.tenant, roles, nil, c.params)
	if err != nil {
		return err
	}
	selector := preps.GetDWSelectorForTest()
	context := mctech.NewBaseContext(false)
	context.(mctech.ModifyContext).SetPrepareResult(result)
	context.(mctech.ModifyContext).SetDWSelector(selector)
	index, err := context.SelectDWIndex()
	if err != nil {
		return err
	}
	require.Equal(t, c.expect, *index, c.Source(i))
	return nil
}
