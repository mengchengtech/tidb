package mctech_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/preps"
	"github.com/stretchr/testify/require"
)

type parserResultCase struct {
	c1           mctech.Comments
	g1           mctech.GlobalValueInfo
	c2           mctech.Comments
	g2           mctech.GlobalValueInfo
	expectFrom   string
	expectPkg    string
	expectGlobal map[string]any
}

func TestParserResult(t *testing.T) {
	testCases := []parserResultCase{
		{nil, nil, nil, nil, "", "", map[string]any{"set": false}},
		{mctech.NewComments("s1", ""), nil, nil, nil, "s1", "", map[string]any{"set": false}},
		{mctech.NewComments("", "p1"), nil, nil, nil, "", "p1", map[string]any{"set": false}},
		{nil, nil, nil, nil, "", "", map[string]any{"set": false}},
		{nil, mctech.NewGlobalValue(true, []string{"t1", "t2"}, nil), nil, nil, "", "", map[string]any{"set": true, "excludes": []string{"t1", "t2"}}},
		{nil, mctech.NewGlobalValue(true, nil, []string{"t3", "t4"}), nil, nil, "", "", map[string]any{"set": true, "includes": []string{"t3", "t4"}}},
		{nil, mctech.NewGlobalValue(true, []string{"t1", "t2"}, []string{"t3", "t4"}), nil, nil, "", "", map[string]any{"set": true, "excludes": []string{"t1", "t2"}, "includes": []string{"t3", "t4"}}},
		{mctech.NewComments("s1", "p1"), nil, nil, nil, "s1", "p1", map[string]any{"set": false}},
		{nil, nil, mctech.NewComments("s2", ""), nil, "s2", "", map[string]any{"set": false}},
		{nil, nil, mctech.NewComments("", "p2"), nil, "", "p2", map[string]any{"set": false}},
		{nil, nil, nil, mctech.NewGlobalValue(true, []string{"t1"}, nil), "", "", map[string]any{"set": true, "excludes": []string{"t1"}}},
		{nil, nil, mctech.NewComments("s2", "p2"), mctech.NewGlobalValue(true, nil, []string{"t2"}), "s2", "p2", map[string]any{"set": true, "includes": []string{"t2"}}},
		{mctech.NewComments("s1", "p1"), mctech.NewGlobalValue(true, []string{"t1"}, []string{"t2"}), mctech.NewComments("s2", "p2"), mctech.NewGlobalValue(true, []string{"t3"}, []string{"t4"}), "s2", "p2", map[string]any{"set": true, "excludes": []string{"t1"}, "includes": []string{"t2"}}},
	}
	var (
		roles mctech.FlagRoles
		err   error
	)
	roles, err = preps.NewFlagRoles(true, false, false)
	require.NoError(t, err)
	for _, testCase := range testCases {
		var (
			r1 mctech.ParseResult
			r2 mctech.ParseResult
		)

		p1 := map[string]any{}
		if testCase.g1 != nil {
			p1["global"] = testCase.g1
		}
		r1, err = mctech.NewParseResult("", roles, testCase.c1, p1)
		require.NoError(t, err)

		p2 := map[string]any{}
		if testCase.g2 != nil {
			p2["global"] = testCase.g2
		}
		r2, err = mctech.NewParseResult("", roles, testCase.c2, p2)
		require.NoError(t, err)

		r2.Merge(r1)
		cc := r2.Comments()
		if testCase.expectFrom == "" {
			require.Nil(t, cc.Service())
		} else {
			require.Equal(t, testCase.expectFrom, cc.Service().From())
		}
		if testCase.expectPkg == "" {
			require.Nil(t, cc.Package())
		} else {
			require.Equal(t, testCase.expectPkg, cc.Package().Name())
		}
		require.Equal(t, testCase.expectGlobal, r2.Global().(mctech.GlobalValueInfoForTest).GetInfoForTest())
	}
}
