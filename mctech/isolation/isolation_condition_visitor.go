package isolation

import (
	"container/list"
	"fmt"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/opcode"
)

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

type databaseNameVisitor struct {
	context mctech.Context
	dbNames map[string]bool // sql中用到的数据库名称
}

type cteScopeItem struct {
	statement ast.Node
	cteNames  []string
}

// Enter implements interface Visitor
func (v *databaseNameVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	var err error
	switch node := n.(type) {
	case *ast.ColumnNameExpr:
		err = v.enterColumnNameExpr(node)
	}
	if err != nil {
		panic(err)
	}
	return n, false
}

// Leave implements interface Visitor
func (v *databaseNameVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	var err error
	switch node := n.(type) {
	case *ast.TableName:
		err = v.leaveTableName(node)
	}
	if err != nil {
		panic(err)
	}
	return n, true
}

func (v *databaseNameVisitor) enterColumnNameExpr(node *ast.ColumnNameExpr) error {
	dbName := node.Name.Schema.L
	if dbName == "" {
		return nil
	}

	// database.table.column
	physicalDbName, err := v.context.ToPhysicalDbName(dbName)
	if err == nil {
		if physicalDbName != dbName {
			node.Name.Schema = model.NewCIStr(physicalDbName)
		}
	}
	return err
}

func (v *databaseNameVisitor) leaveTableName(node *ast.TableName) error {
	dbName := node.Schema.L
	if dbName != "" {
		physicalDbName, err := v.context.ToPhysicalDbName(dbName)
		if err != nil {
			return err
		}

		if physicalDbName != dbName {
			node.Schema = model.NewCIStr(physicalDbName)
		}
	}
	v.dbNames[node.Schema.L] = true
	return nil
}

type isolationConditionVisitor struct {
	*databaseNameVisitor
	// 租户条件是否使用参数化方式
	usingParam bool
	tenant     ast.ValueExpr
	excludes   []ast.ExprNode

	withClauseScope     *nodeScope[*cteScopeItem]
	columnModifiedScope *nodeScope[bool]
}

const tenantFieldName = "tenant"

func newIsolationConditionVisitor(
	mctechCtx mctech.Context,
	charset string, collation string) *isolationConditionVisitor {
	visitor := &isolationConditionVisitor{
		usingParam: mctechCtx.UsingTenantParam(),
		databaseNameVisitor: &databaseNameVisitor{
			context: mctechCtx,
			dbNames: map[string]bool{},
		},
		withClauseScope:     &nodeScope[*cteScopeItem]{items: list.New()},
		columnModifiedScope: &nodeScope[bool]{items: list.New()},
	}
	result := mctechCtx.PrepareResult()
	if result.Global() {
		length := len(result.Excludes())
		if length > 0 {
			exprList := make([]ast.ExprNode, length)
			for i, str := range result.Excludes() {
				// global相关的，只能生成常量
				exprList[i] = ast.NewValueExpr(str, charset, collation)
			}
			visitor.excludes = exprList
		}
	} else if !visitor.usingParam {
		// 非参数化租户过滤条件，且tenant不为空时
		tenant := result.Tenant()
		if tenant != "" {
			visitor.tenant = ast.NewValueExpr(tenant, charset, collation)
		}
	}

	return visitor
}

func (v *isolationConditionVisitor) enabled() bool {
	return v.usingParam || v.tenant != nil || len(v.excludes) > 0
}

// Enter implements interface Visitor
func (v *isolationConditionVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	v.databaseNameVisitor.Enter(n)

	if v.enabled() {
		switch node := n.(type) {
		case
			*ast.UpdateStmt, *ast.DeleteStmt, *ast.SelectStmt,
			*ast.SetOprSelectList, *ast.SetOprStmt: // InsertStmt不支持With
			v.enterWithScope(node)
		case *ast.WithClause:
			v.setWithClause(node)
		case *ast.InsertStmt: // include replace / insert .... duplicate
			v.enterInsertStatement(node)
		case *ast.SubqueryExpr: // subquery
			v.enterSubquery(node)
		case *ast.TableSource:
			v.enterTableSource(node)
		}
	}

	return n, false
}

// Leave implements interface Visitor
func (v *isolationConditionVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	v.databaseNameVisitor.Leave(n)
	if v.enabled() {
		switch node := n.(type) {
		case
			*ast.SetOprSelectList, *ast.SetOprStmt:
			v.leaveWithScope(node)
		case *ast.DeleteStmt:
			v.leaveDeleteStatement(node)
			v.leaveWithScope(node)
		case *ast.UpdateStmt:
			v.leaveUpdateStatement(node)
			v.leaveWithScope(node)
		case *ast.InsertStmt: // include replace / insert .... duplicate
			v.leaveInsertStatement(node)
		case *ast.SelectStmt:
			v.leaveSelectStatement(node)
			v.leaveWithScope(node)
		case *ast.ValuesExpr: // values ()
		case *ast.SubqueryExpr: // subquery
			v.leaveSubquery(node)
		case *ast.TableSource:
			v.leaveTableSource(node)
		}
	}

	return n, true
}

func (v *isolationConditionVisitor) enterWithScope(stmt ast.Node) {
	v.withClauseScope.Push(&cteScopeItem{
		statement: stmt,
	})
}

func (v *isolationConditionVisitor) setWithClause(withClause *ast.WithClause) {
	cteNames := make([]string, len(withClause.CTEs))
	for i, cte := range withClause.CTEs {
		rawName := cte.Name.L
		cteNames[i] = rawName
	}

	item := v.withClauseScope.Peek()
	item.cteNames = cteNames
}

func (v *isolationConditionVisitor) leaveWithScope(node ast.Node) {
	item := v.withClauseScope.Peek()
	if item.statement != node {
		return
	}
	v.withClauseScope.Pop()
}

func (v *isolationConditionVisitor) enterInsertStatement(node *ast.InsertStmt) {
	source := node.Table.TableRefs.Left.(*ast.TableSource)
	tableName := source.Source.(*ast.TableName)
	dbName := tableName.Schema.L

	sd := v.context
	if dbName == "" {
		dbName = sd.CurrentDB()
	}

	// 只处理global_xxxx的表
	if sd.PrepareResult().Global() || !sd.IsGlobalDb(dbName) {
		return
	}

	var modified bool
	if len(node.Setlist) == 0 {
		// insert .... values/select ....
		modified = v.processInsertColumns(node)
	} else {
		for _, set := range node.Setlist {
			if set.Column.Name.L == tenantFieldName {
				// 存在tenant字段，不处理
				return
			}
		}

		node.Setlist = append(node.Setlist, &ast.Assignment{
			Column: &ast.ColumnName{
				Name: model.NewCIStr(tenantFieldName),
			},
			Expr: v.createTenantExpr(),
		})
	}
	v.columnModifiedScope.Push(modified)
}

func (v *isolationConditionVisitor) leaveInsertStatement(node *ast.InsertStmt) {
	v.columnModifiedScope.Pop()
}

func (v *isolationConditionVisitor) enterSubquery(node *ast.SubqueryExpr) {
	// 子查询不应该受 insert/upsert 的列修改影响
	v.columnModifiedScope.Push(false)
}

func (v *isolationConditionVisitor) leaveSubquery(node *ast.SubqueryExpr) {
	v.columnModifiedScope.Pop()
}

func (v *isolationConditionVisitor) enterTableSource(node *ast.TableSource) {
	if _, ok := node.Source.(*ast.TableName); !ok {
		// 子查询不应该受 insert/upsert 的列修改影响
		v.columnModifiedScope.Push(false)
	}
}

func (v *isolationConditionVisitor) leaveTableSource(node *ast.TableSource) {
	if _, ok := node.Source.(*ast.TableName); !ok {
		// 子查询不应该受 insert/upsert 的列修改影响
		v.columnModifiedScope.Pop()
	}
}

func (v *isolationConditionVisitor) leaveDeleteStatement(node *ast.DeleteStmt) {
	var condition ast.ExprNode
	if node.TableRefs != nil {
		condition = v.processFromClause(node.TableRefs)
	}
	node.Where = v.createAndCondition(condition, node.Where)
}

func (v *isolationConditionVisitor) leaveUpdateStatement(node *ast.UpdateStmt) {
	var condition ast.ExprNode
	if node.TableRefs != nil {
		condition = v.processFromClause(node.TableRefs)
	}
	node.Where = v.createAndCondition(condition, node.Where)
}

func (v *isolationConditionVisitor) leaveSelectStatement(node *ast.SelectStmt) {
	v.processSelectItems(node)
	condition := v.processFromClause(node.From)
	node.Where = v.createAndCondition(condition, node.Where)
}

// -------------------------------------------------------------------------------

func (v *isolationConditionVisitor) createAndCondition(left ast.ExprNode, right ast.ExprNode) ast.ExprNode {
	if left == nil {
		return right
	} else if right == nil {
		return left
	} else {
		// left, right 都不为nil
		return &ast.BinaryOperationExpr{
			L:  left,
			Op: opcode.LogicAnd,
			R:  right,
		}
	}
}

func (v *isolationConditionVisitor) processFromClause(fromClause *ast.TableRefsClause) ast.ExprNode {
	var condition ast.ExprNode
	if fromClause != nil {
		tableRefs := fromClause.TableRefs
		condition = v.processJoinClause(tableRefs)
	}
	return condition
}

func (v *isolationConditionVisitor) processSelectItems(node *ast.SelectStmt) {
	modifiedColumns := v.columnModifiedScope.Peek()
	if modifiedColumns {
		// 作为insert/upsert的子查询才需要考虑添加'tenant'字段
		items := node.Fields.Fields
		// 检查SELECT子句中包含别名为'tenant'或者列名为'tenant', '*'的
		var hasTenant bool
		for _, item := range items {
			alias := item.AsName.L
			// SELECT xxx as tenant
			// SELECT tenant, ......
			var hasTenantItem bool
			if alias != "" {
				hasTenantItem = tenantFieldName == alias
			} else {
				if colExpr, ok := item.Expr.(*ast.ColumnNameExpr); ok {
					hasTenantItem = tenantFieldName == colExpr.Name.Name.L
				}
			}

			// SELECT *
			if hasTenantItem {
				hasTenant = true
				break
			}
		}

		if !hasTenant {
			node.Fields.Fields = append(items, &ast.SelectField{
				Expr:   v.createTenantExpr(),
				AsName: model.NewCIStr(tenantFieldName),
			})
		}
	}
}

func (v *isolationConditionVisitor) processTableSource(
	source *ast.TableSource, condition ast.ExprNode) ast.ExprNode {
	// 普通join方式，条件添加到每个ON后面
	if table, ok := source.Source.(*ast.TableName); ok {
		cond := v.createTenantConditionFromTable(table, source.AsName)
		return v.createAndCondition(cond, condition)
	}

	// 非表/视图（一般来说是内联查询）不在此处理
	return condition
}

func (v *isolationConditionVisitor) processJoinClause(tableRefs *ast.Join) ast.ExprNode {
	// 无法添加到join on 里的条件
	var condition ast.ExprNode
	if source, ok := tableRefs.Right.(*ast.TableSource); ok {
		if tableRefs.On != nil {
			cond := tableRefs.On.Expr
			expr := v.processTableSource(source, cond)
			tableRefs.On.Expr = expr
		} else {
			// using (....)
			// 条件加到 where后面
			condition = v.processTableSource(source, nil)
		}
	}

	// ok == false, 目前发现只会出现在from后的第一张表
	// 此时可能的类型为 *ast.TableSource, *ast.SelectStmt, .... 其它非Join类型
	if join, ok := tableRefs.Left.(*ast.Join); ok {
		cond := v.processJoinClause(join)
		// cond != nil 时, 上一层解析完后返回的无法添加到 join on 里的条件
		// 与当前层的合并
		condition = v.createAndCondition(cond, condition)
	} else if source, ok := tableRefs.Left.(*ast.TableSource); ok {
		// 此时可能的类型为 *ast.TableSource, *ast.SelectStmt, ......
		// 只处理TableSource这一种情况
		condition = v.processTableSource(source, condition)
	}
	return condition
}

func (v *isolationConditionVisitor) createTenantConditionFromTable(
	table *ast.TableName, alias model.CIStr) ast.ExprNode {
	dbName := table.Schema.O
	tableName := table.Name.L

	sd := v.context
	if dbName == "" {
		// dbName为空时，有可能是视图的引用
		if v.withClauseScope.Size() > 0 {
			lst := v.withClauseScope.Entries()
			el := lst.Front()
			for el != nil {
				withCTE := el.Value.(*cteScopeItem)
				for _, a := range withCTE.cteNames {
					if a == tableName {
						// 视图引用，不用处理
						return nil
					}
				}
				el = el.Next()
			}
		}
		dbName = sd.CurrentDB()
	}

	if !sd.IsGlobalDb(dbName) {
		// 只处理global_xxxx的表
		return nil
	}
	colName := ast.ColumnName{Name: model.NewCIStr(tenantFieldName)}
	if alias.O != "" {
		colName.Table = alias
	} else {
		colName.Table = table.Name
		colName.Schema = table.Schema
	}
	tenantField := &ast.ColumnNameExpr{
		Name: &colName,
	}

	var condition ast.ExprNode
	rt := sd.PrepareResult()
	if rt.Global() {
		if len(v.excludes) > 0 {
			condition = &ast.PatternInExpr{
				Expr: tenantField,
				Not:  true,
				List: v.excludes,
			}
		}
	} else {
		condition = &ast.BinaryOperationExpr{
			L:  tenantField,
			Op: opcode.EQ,
			R:  v.createTenantExpr(),
		}
	}
	return condition
}

/**
 * 处理insert/upsert的列，添加tenant字段
 */
func (v *isolationConditionVisitor) processInsertColumns(node *ast.InsertStmt) bool {
	columns := node.Columns
	if len(columns) == 0 {
		panic(fmt.Errorf("insert/upsert语句缺少列定义，无法处理租户信息"))
	}

	var modified = true
	for _, c := range columns {
		if c.Name.L == tenantFieldName {
			modified = false
			break
		}
	}

	if modified {
		node.Columns = append(node.Columns, &ast.ColumnName{
			Name: model.NewCIStr(tenantFieldName),
		})

		operands := node.Lists
		length := len(operands)
		for i := 0; i < length; i++ {
			operands[i] = append(operands[i], v.createTenantExpr())
		}
	}
	return modified
}

func (v *isolationConditionVisitor) createTenantExpr() (expr ast.ValueExpr) {
	if v.usingParam {
		expr = ast.NewParamMarkerExpr(mctech.ExtensionParamMarkerOffset)
	} else {
		expr = v.tenant
	}
	return expr
}
