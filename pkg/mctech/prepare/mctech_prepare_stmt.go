package prepare

import "github.com/pingcap/tidb/pkg/parser/ast"

// BinaryPrepareStmt binary protocol PrepareStmt struct
type BinaryPrepareStmt struct {
	ast.PrepareStmt
	PrepStmt any
}
