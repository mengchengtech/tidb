package tenant

import (
	"github.com/pingcap/tidb/mctech"
)

type mctechTestCase struct {
	shortDb string
	src     string
	expect  string
}

type testMCTechContext struct {
	mctech.MCTechContext
	currentDb string
}

type testDBSelector struct {
	dbIndex mctech.DbIndex
}

func (s *testDBSelector) GetDbIndex() (mctech.DbIndex, error) {
	return s.dbIndex, nil
}

func newTestMCTechContext(currentDb string) mctech.MCTechContext {
	result := mctech.NewResolveResult("gslq4dev", map[string]any{
		"dbPrefix": "mock",
		"global":   &mctech.GlobalValueInfo{Global: false},
	})

	context := &testMCTechContext{
		MCTechContext: mctech.NewBaseMCTechContext(
			result, &testDBSelector{dbIndex: 1}),
	}

	context.currentDb = context.ToPhysicalDbName(currentDb)
	return context
}

func (d *testMCTechContext) CurrentDB() string {
	return d.currentDb
}
