package visitor

import (
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"golang.org/x/exp/slices"
)

// ApplyExtension apply tenant condition
func ApplyExtension(mctx mctech.Context, node ast.Node,
	charset, collation string) (schema mctech.StmtSchemaInfo, skipped bool, err error) {
	skipped = false
	switch stmtNode := node.(type) {
	case *ast.SelectStmt:
		schema, err = doApplyExtension(mctx, stmtNode, charset, collation)
		if stmtNode.Kind == ast.SelectStmtKindTable {
			// "desc global_xxx.table" 语句解析后生成的SelectStmt
			skipped = true
		}
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.InsertStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt,
		*ast.LoadDataStmt,
		*ast.NonTransactionalDMLStmt, // BATCH ......
		*ast.TruncateTableStmt:
		schema, err = doApplyExtension(mctx, stmtNode, charset, collation)
	case *ast.MCTechStmt:
		// MCTechStmt只需要处理对应的子句就可以
		schema, skipped, err = ApplyExtension(mctx, stmtNode.Stmt, charset, collation)
	case *ast.ExplainStmt:
		// ExplainStmt只需要处理对应的子句就可以
		schema, skipped, err = ApplyExtension(mctx, stmtNode.Stmt, charset, collation)
	default:
		skipped = true
	}

	if schema.Databases == nil {
		schema.Databases = []string{}
	}
	if schema.Tables == nil {
		schema.Tables = []mctech.TableName{}
	}
	return schema, skipped, err
}

func doApplyExtension(
	mctx mctech.Context, node ast.Node, charset, collation string) (schema mctech.StmtSchemaInfo, err error) {
	failpoint.Inject("SetSQLDBS", func(v failpoint.Value) {
		str := v.(string)
		for _, item := range strings.Split(str, ",") {
			if !slices.Contains(schema.Databases, item) {
				schema.Databases = append(schema.Databases, item)
			}
			schema.Tables = []mctech.TableName{}
		}
		schema.Sort()
		failpoint.Return(schema, nil)
	})

	var v tblNameVisitor
	if mctx.InExecute() {
		v = newTableNameVisitor(mctx)
	} else {
		v = newIsolationConditionVisitor(mctx, charset, collation)
	}
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	node.Accept(v)
	schema = v.StmtSchemaInfo()
	schema.Sort()

	return schema, err
}
