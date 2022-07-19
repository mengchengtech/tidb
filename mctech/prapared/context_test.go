package prapared

import (
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/stretchr/testify/require"
)

type testContextCase struct {
	tenant   string
	response string
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
		{"gslq", `{"current": ""}`, 1, map[string]any{"background": true}, "cannot unmarshal"},
		{"gslq", `{"db": ""}`, 1, map[string]any{"requestId": "12345678"}, "cannot unmarshal"},
		{"gslq", `{"current": 2}`, 2, nil, ""},
		{"gslq", `{"current": 2}`, 2, nil, ""}, // 测试缓存中获取
		{"gslq", `{"current": 2}`, 1, map[string]any{"background": true}, ""},
		{"gslq", `{"db": 1}`, 1, map[string]any{"requestId": "12345678"}, ""},
		{"gslq", `{"db": 1}`, 1, map[string]any{"requestId": "12345678"}, ""}, // 测试重复执行缓存
	}

	doRunTest(t, contextRunTestCase, cases)
}

func contextRunTestCase(t *testing.T, c *testContextCase) error {
	var rpcClient = mctech.GetRPCClient()
	mctech.SetRPCClientForTest(&mockClient{})
	defer mctech.SetRPCClientForTest(rpcClient)
	getDoFunc = createGetDoFunc(c.response)

	result, err := mctech.NewPrepareResult("gslq", c.params)
	if err != nil {
		return err
	}
	selector := newDBSelector(result)
	index, err := selector.GetDbIndex()
	if err != nil {
		return err
	}
	require.Equal(t, c.expect, index, c.Source())
	return nil
}
