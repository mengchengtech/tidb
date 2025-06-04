package visitor

import (
	"container/list"
	"slices"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type tblNameVisitor interface {
	ast.Visitor
	StmtSchemaInfo() mctech.StmtSchemaInfo
}

func newTableNameVisitor(mctx mctech.Context) *tableNameVisitor {
	return &tableNameVisitor{
		context:    mctx,
		tableNames: map[string]mctech.TableName{},

		withClauseScope: &nodeScope[*cteScopeItem]{items: list.New()},
	}
}

type nodeScope[T any] struct {
	items *list.List
}

func (s nodeScope[T]) Size() int {
	return s.items.Len()
}

func (s nodeScope[T]) Push(item T) {
	s.items.PushFront(item)
}

func (s nodeScope[T]) Pop() T {
	first := s.items.Front()
	var v T
	if first == nil {
		return v
	}
	v = first.Value.(T)
	s.items.Remove(first)
	return v
}

func (s nodeScope[T]) Peek() T {
	first := s.items.Front()
	var v T
	if first == nil {
		return v
	}
	return first.Value.(T)
}

func (s nodeScope[T]) Entries() *list.List {
	return s.items
}

type tableNameVisitor struct {
	context    mctech.Context
	tableNames map[string]mctech.TableName // sql中用到的数据库物理表的全名称

	withClauseScope *nodeScope[*cteScopeItem]
}

type cteScopeItem struct {
	statement ast.Node
	cteNames  []string
}

func (v *tableNameVisitor) StmtSchemaInfo() mctech.StmtSchemaInfo {
	schema := mctech.StmtSchemaInfo{}
	for _, table := range v.tableNames {
		schema.Tables = append(schema.Tables, table)
		if !slices.Contains(schema.Databases, table.Database) {
			schema.Databases = append(schema.Databases, table.Database)
		}
	}
	return schema
}

// Enter implements interface Visitor
func (v *tableNameVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	var err error
	switch node := n.(type) {
	case *ast.ColumnName:
		err = v.enterColumnName(node)
	case
		*ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt: // InsertStmt不支持With
		v.enterWithScope(node)
	case *ast.CommonTableExpression:
		v.addCTE(node)
	}
	if err != nil {
		panic(err)
	}
	return n, false
}

// Leave implements interface Visitor
func (v *tableNameVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	var err error
	switch node := n.(type) {
	case
		*ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt,
		*ast.SetOprSelectList, *ast.SetOprStmt: // InsertStmt 不支持with
		v.leaveWithScope(node)
	case *ast.TableName:
		err = v.leaveTableName(node)
	}
	if err != nil {
		panic(err)
	}
	return n, true
}

func (v *tableNameVisitor) enterWithScope(stmt ast.Node) {
	v.withClauseScope.Push(&cteScopeItem{
		statement: stmt,
	})
}

func (v *tableNameVisitor) addCTE(cte *ast.CommonTableExpression) {
	item := v.withClauseScope.Peek()
	item.cteNames = append(item.cteNames, cte.Name.L)
}

func (v *tableNameVisitor) leaveWithScope(node ast.Node) {
	item := v.withClauseScope.Peek()
	if item.statement != node {
		return
	}
	v.withClauseScope.Pop()
}

func (v *tableNameVisitor) fetchDbName(table *ast.TableName) (dbName string, isCteName bool) {
	dbName = table.Schema.O
	isCteName = false

	if dbName == "" {
		// dbName为空时，有可能是视图的引用
		tableName := table.Name.L
		if v.withClauseScope.Size() > 0 {
			lst := v.withClauseScope.Entries()
			el := lst.Front()
			for el != nil {
				withCTE := el.Value.(*cteScopeItem)
				if slices.Contains(withCTE.cteNames, tableName) {
					// 的确是视图引用
					isCteName = true
					break
				}
				el = el.Next()
			}
		}

		if !isCteName {
			// 不是cte，且表信息中没有数据库前缀，使用当前会话上的默认数据库
			dbName = v.context.CurrentDB()
		}
	}
	return dbName, isCteName
}

func (v *tableNameVisitor) enterColumnName(node *ast.ColumnName) error {
	dbName := node.Schema.L
	if dbName == "" {
		return nil
	}

	// database.table.column
	physicalDbName, err := v.context.ToPhysicalDbName(dbName)
	if err == nil {
		if physicalDbName != dbName {
			node.Schema = model.NewCIStr(physicalDbName)
		}
	}
	return err
}

func (v *tableNameVisitor) leaveTableName(node *ast.TableName) error {
	dbName, isCteName := v.fetchDbName(node)
	if isCteName {
		// 跳过视图
		return nil
	}

	if dbName != "" {
		physicalDbName, err := v.context.ToPhysicalDbName(dbName)
		if err != nil {
			return err
		}

		if physicalDbName != dbName {
			dbName = physicalDbName
			node.Schema = model.NewCIStr(physicalDbName)
		}
	}
	key := dbName + "|" + node.Name.L
	v.tableNames[key] = mctech.TableName{Database: dbName, Table: node.Name.L}
	return nil
}
