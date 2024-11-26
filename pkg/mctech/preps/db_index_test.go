package preps_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/mctech/preps"
	"github.com/stretchr/testify/require"
)

type testContextCase struct {
	tenant   string
	response map[string]any
	expect   mctech.DbIndex
	params   map[string]any
	failure  string
}

func (c *testContextCase) Failure() string {
	return c.failure
}

func (c *testContextCase) Source() any {
	return c.response
}

func TestDbSelectorGetDbIndex(t *testing.T) {
	cases := []*testContextCase{
		{"gslq", map[string]any{"DbIndex.CurrentDB": map[string]any{"current": ""}}, 1, map[string]any{"background": true}, "cannot unmarshal"},
		{"gslq", map[string]any{"DbIndex.DBByRequest": map[string]any{"db": ""}}, 1, map[string]any{"requestId": "12345678"}, "cannot unmarshal"},
		{"gslq", map[string]any{"DbIndex.CurrentDB": map[string]any{"current": 2}}, 2, map[string]any{}, ""},
		{"gslq", map[string]any{"DbIndex.CurrentDB": map[string]any{"current": 2}}, 2, map[string]any{}, ""}, // 测试缓存中获取
		{"gslq", map[string]any{"DbIndex.CurrentDB": map[string]any{"current": 2}}, 1, map[string]any{"background": true}, ""},
		{"gslq", map[string]any{"DbIndex.DBByRequest": map[string]any{"db": 1}}, 1, map[string]any{"requestId": "12345678"}, ""},
		{"gslq", map[string]any{"DbIndex.DBByRequest": map[string]any{"db": 1}}, 1, map[string]any{"requestId": "12345678"}, ""}, // 测试重复执行缓存
	}

	doRunTest(t, contextRunTestCase, cases)
}

func contextRunTestCase(t *testing.T, c *testContextCase) error {
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mock.M(t, c.response),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")
	result, err := mctech.NewPrepareResult(c.tenant, true, c.params)
	if err != nil {
		return err
	}
	selector := preps.NewDBSelectorForTest(result)
	index, err := selector.GetDbIndex()
	if err != nil {
		return err
	}
	require.Equal(t, c.expect, index, c.Source())
	return nil
}
