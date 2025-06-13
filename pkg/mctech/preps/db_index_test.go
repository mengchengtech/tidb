package preps_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/mctech/preps"
	"github.com/pingcap/tidb/pkg/sessionctx"
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

func contextRunTestCase(t *testing.T, i int, c *testContextCase, sctx sessionctx.Context) error {
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mock.M(t, c.response),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")

	roles, err := preps.NewFlagRoles(true, false, false)
	if err != nil {
		return err
	}
	result, err := mctech.NewParseResult(c.tenant, roles, nil, c.params)
	if err != nil {
		return err
	}
	selector := preps.GetDWSelectorForTest()
	mctx, err := mctech.WithNewContext(sctx)
	if err != nil {
		return err
	}
	modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetParseResult(result)
	modifyCtx.SetDWSelector(selector)
	index, err := mctx.SelectDWIndex()
	if err != nil {
		return err
	}
	require.Equal(t, c.expect, *index, c.Source(i))
	return nil
}
