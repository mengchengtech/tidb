// add by zhangbing

package ast_test

import (
	"testing"

	. "github.com/pingcap/tidb/pkg/parser/ast"
)

func TestMCTechFuncCallExprRestore(t *testing.T) {
	testCases := []NodeRestoreTestCase{
		{"mctech_sequence()", "MCTECH_SEQUENCE()"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	runNodeRestoreTest(t, testCases, "select %s", extractNodeFunc)
}
