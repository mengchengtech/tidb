package misc_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/misc"
	"github.com/pingcap/tidb/pkg/mctech/preps"
	"github.com/pingcap/tidb/pkg/parser"
	. "github.com/pingcap/tidb/pkg/parser/format"
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

type miscMCTechTestCase struct {
	sql     string
	expect  string
	failure string
}

func (t miscMCTechTestCase) Source() any {
	return t.sql
}

func (t *miscMCTechTestCase) Expect() string {
	return t.expect
}

func (t *miscMCTechTestCase) Failure() string {
	return t.failure
}

var (
	useCases = []*miscMCTechTestCase{
		{"use global_platform", "USE `mock_global_platform`", ""},
		{"use global_dw", "USE `mock_global_dw_1`", ""},
		{"show tables in global_dw", "SHOW TABLES IN `mock_global_dw_1`", ""},
		{"analyze table global_dw.table1", "ANALYZE TABLE `mock_global_dw_1`.`table1`", ""},
	}
)

var miscExtensionCases = [][]*miscMCTechTestCase{
	useCases,
}

func TestMiscExtensionVisitor(t *testing.T) {
	for _, lst := range miscExtensionCases {
		doRunTest(t, doRunMiscMCTechTestCase, lst)
	}
}

func doRunMiscMCTechTestCase(t *testing.T, c *miscMCTechTestCase) error {
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

		if _, err := misc.ApplyExtension(mctechCtx, stmt); err != nil {
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
	result, err := mctech.NewPrepareResult("gslq4dev", preps.NewFlagRoles(true, false), nil, map[string]any{
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