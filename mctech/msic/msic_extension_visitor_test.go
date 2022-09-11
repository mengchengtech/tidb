package msic

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser"
	. "github.com/pingcap/tidb/parser/format"
	"github.com/stretchr/testify/require"
)

type testMCTechContext struct {
	mctech.Context
}

func (d *testMCTechContext) BaseContext() mctech.Context {
	return d.Context
}

type testDBSelector struct {
	dbIndex mctech.DbIndex
}

func (s *testDBSelector) GetDbIndex() (mctech.DbIndex, error) {
	return s.dbIndex, nil
}

type msicMCTechTestCase struct {
	sql     string
	expect  string
	failure string
}

func (t msicMCTechTestCase) Source() any {
	return t.sql
}

func (t *msicMCTechTestCase) Expect() string {
	return t.expect
}

func (t *msicMCTechTestCase) Failure() string {
	return t.failure
}

var (
	useCases = []*msicMCTechTestCase{
		{"use global_platform", "USE `mock_global_platform`", ""},
	}
)

var msicExtensionCases = [][]*msicMCTechTestCase{
	useCases,
}

func TestMsicExtensionVisitor(t *testing.T) {
	for _, lst := range msicExtensionCases {
		doRunTest(t, doRunMsicMCTechTestCase, lst)
	}
}

func doRunMsicMCTechTestCase(t *testing.T, c *msicMCTechTestCase) error {
	p := parser.New()
	stmts, _, err := p.Parse(c.sql, "", "")
	require.NoErrorf(t, err, "source %v", c.sql)
	var sb strings.Builder
	restoreSQLs := ""
	for _, stmt := range stmts {
		sb.Reset()
		mctechCtx, err := newTestMCTechContext()
		if err != nil {
			return err
		}

		if _, err := ApplyExtension(mctechCtx, stmt); err != nil {
			return err
		}
		err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreBracketAroundBinaryOperation, &sb))
		if err != nil {
			return err
		}

		restoreSQL := sb.String()
		if restoreSQLs != "" {
			restoreSQLs += "; "
		}
		restoreSQLs += restoreSQL

	}
	require.Equalf(t, c.expect, restoreSQLs, "restore %v; expect %v", restoreSQLs, c.expect)
	return nil
}

func newTestMCTechContext() (mctech.Context, error) {
	result, err := mctech.NewPrepareResult("gslq4dev", map[string]any{
		"dbPrefix": "mock",
	})

	if err != nil {
		return nil, err
	}

	context := &testMCTechContext{
		Context: mctech.NewBaseContext(false),
	}
	modifyCtx := context.Context.(mctech.ModifyContext)
	modifyCtx.SetPrepareResult(result)
	modifyCtx.SetDBSelector(&testDBSelector{dbIndex: 1})
	return context, err
}
