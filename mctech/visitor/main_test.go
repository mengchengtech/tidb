package visitor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/preps"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/stretchr/testify/require"
)

var dbMap = map[string]string{
	"pf": "global_platform",
	"dw": "global_dw",
}

type mctechTestCase interface {
	Source() any
	Expect() string
	Failure() string
}

type runTestCaseType[T mctechTestCase] func(t *testing.T, tbl T) error

func doRunTest[T mctechTestCase](t *testing.T, runTestCase runTestCaseType[T], cases []T) {
	for _, c := range cases {
		err := runTestCase(t, c)
		failure := c.Failure()
		if err == nil && failure == "" {
			continue
		}

		if failure != "" {
			require.ErrorContainsf(t, err, failure, "source %v", c.Source())
		} else {
			require.NoErrorf(t, err, "source %v", c.Source())
		}
	}
}

type testMCTechContext struct {
	mctech.Context
	currentDb string
}

func (d *testMCTechContext) GetInfoForTest() string {
	info := d.Context.(mctech.ContextForTest).GetInfoForTest()
	return fmt.Sprintf("{%s,%s}", info, d.CurrentDB())
}

func (d *testMCTechContext) CurrentDB() string {
	return d.currentDb
}

func (d *testMCTechContext) BaseContext() mctech.Context {
	return d.Context
}

type testDWSelector struct {
	dwIndex mctech.DWIndex
}

func (s *testDWSelector) SelectIndex(dbPrefix, requestID string, forcebackground bool) (*mctech.DWIndex, error) {
	return &s.dwIndex, nil
}

func (s *testDWSelector) GetIndexInfo(dbPrefix string) (*mctech.DWIndexInfo, error) {
	return &mctech.DWIndexInfo{
		Current:    s.dwIndex,
		Background: s.dwIndex ^ 0x0003,
	}, nil
}

func newTestMCTechContext(currentDb, mock string, global bool, excludes, includes []string) (mctech.Context, error) {
	var tenant string
	if !global {
		tenant = "gslq4dev"
	}
	roles, err := preps.NewFlagRoles(true, false, true)
	if err != nil {
		return nil, err
	}
	result, err := mctech.NewPrepareResult(tenant, roles, nil, map[string]any{
		"dbPrefix": mock,
		"global":   mctech.NewGlobalValue(global, excludes, includes),
	})

	if err != nil {
		return nil, err
	}

	context := &testMCTechContext{
		Context: mctech.NewBaseContext(false),
	}
	modifyCtx := context.Context.(mctech.ModifyContext)
	modifyCtx.SetPrepareResult(result)
	modifyCtx.SetDWSelector(&testDWSelector{dwIndex: 1})

	context.currentDb, err = context.ToPhysicalDbName(currentDb)
	return context, err
}
