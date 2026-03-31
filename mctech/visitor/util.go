package visitor

import (
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"golang.org/x/exp/slices"
)

// ApplyExtension apply tenant condition
func ApplyExtension(mctx mctech.Context, stmt ast.StmtNode,
	charset string, collation string) (schema mctech.StmtSchemaInfo, skipped bool, err error) {
	ext := getCommonExtension(mctx, stmt, charset, collation)
	return ext.Apply()
}

func getCommonExtension(mctx mctech.Context, stmt ast.StmtNode, charset, collation string) *_commonExtension {
	return &_commonExtension{
		mctx:      mctx,
		stmt:      stmt,
		visitor:   nil,
		charset:   charset,
		collation: collation,
	}
}

type _commonExtension struct {
	mctx      mctech.Context
	stmt      ast.StmtNode
	visitor   ast.Visitor
	charset   string
	collation string
}

// Apply apply tenant condition
func (c *_commonExtension) Apply() (schema mctech.StmtSchemaInfo, skipped bool, err error) {
	return c.doApply(c.stmt)
}

func (c *_commonExtension) doApply(stmt ast.StmtNode) (schema mctech.StmtSchemaInfo, skipped bool, err error) {
	skipped = false
	switch stmtNode := stmt.(type) {
	case *ast.SelectStmt:
		schema, err = c.doApplyExtension(stmtNode)
		if stmtNode.Kind == ast.SelectStmtKindTable {
			// "desc global_xxx.table" 语句解析后生成的SelectStmt
			skipped = true
		}
	case *ast.UpdateStmt, *ast.DeleteStmt, *ast.InsertStmt,
		*ast.SetOprStmt,
		*ast.LoadDataStmt,
		*ast.NonTransactionalDMLStmt, // BATCH ......
		*ast.TruncateTableStmt:
		schema, err = c.doApplyExtension(stmtNode)
	case *ast.MCTechStmt:
		// MCTechStmt只需要处理对应的子句就可以
		if stmtNode.ShowDesc == nil {
			skipped = true
		} else {
			schema, skipped, err = c.doApply(stmtNode.ShowDesc.Stmt)
		}
	case *ast.ExplainStmt:
		// ExplainStmt只需要处理对应的子句就可以
		schema, skipped, err = c.doApply(stmtNode.Stmt)
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

func (c *_commonExtension) doApplyExtension(stmtNode ast.StmtNode) (schema mctech.StmtSchemaInfo, err error) {
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
	if c.mctx.InExecute() {
		v = newTableNameVisitor(c.mctx)
	} else {
		v = newIsolationConditionVisitor(c.mctx, c.charset, c.collation)
	}
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	stmtNode.Accept(v)
	schema = v.StmtSchemaInfo()
	schema.Sort()

	return schema, err
}
