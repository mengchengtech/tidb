package visitor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	. "github.com/pingcap/tidb/parser/format"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/stretchr/testify/require"
)

var dbMap = map[string]string{
	"pf": "global_platform",
	"dw": "global_dw",
}

type mctechTestCase struct {
	global   bool
	excludes []string
	shortDb  string
	src      string
	expect   string
}

type CreateVisitorFunc func(tbl *mctechTestCase) (ast.Visitor, error)

func doNodeVisitorRunTest(t *testing.T, cases []*mctechTestCase, enableWindowFunc bool, createVisitor CreateVisitorFunc) {
	p := parser.New()
	p.EnableWindowFunc(true)
	for _, tbl := range cases {
		stmts, _, err := p.Parse(tbl.src, "", "")
		require.NoErrorf(t, err, "source %v", tbl.src)
		var sb strings.Builder
		p := parser.New()
		p.EnableWindowFunc(enableWindowFunc)
		comment := fmt.Sprintf("source %v", tbl.src)
		restoreSQLs := ""
		for _, stmt := range stmts {
			sb.Reset()
			visitor, err := createVisitor(tbl)
			if err != nil {
				require.NoError(t, err, comment)
			}
			stmt.Accept(visitor)
			err = stmt.Restore(NewRestoreCtx(DefaultRestoreFlags|RestoreBracketAroundBinaryOperation, &sb))
			require.NoError(t, err, comment)
			restoreSQL := sb.String()
			comment = fmt.Sprintf("source %v; restore %v", tbl.src, restoreSQL)
			if restoreSQLs != "" {
				restoreSQLs += "; "
			}
			restoreSQLs += restoreSQL

		}
		require.Equalf(t, tbl.expect, restoreSQLs, "restore %v; expect %v", restoreSQLs, tbl.expect)
	}
}
