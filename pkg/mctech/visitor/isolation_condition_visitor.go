package visitor

import (
	"container/list"
	"fmt"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

type isolationConditionVisitor struct {
	*databaseNameVisitor
	// 租户条件是否使用参数化方式
	usingParam bool
	tenant     ast.ValueExpr
	excludes   []ast.ExprNode
	includes   []ast.ExprNode

	columnModifiedScope *nodeScope[bool]
}

const tenantFieldName = "tenant"

type dbNameVisitor interface {
	ast.Visitor
	DBNames() map[string]bool
}

func newDatabaseNameVisitor(mctx mctech.Context) *databaseNameVisitor {
	return &databaseNameVisitor{
		context: mctx,
		dbNames: map[string]bool{},

		withClauseScope: &nodeScope[*cteScopeItem]{items: list.New()},
	}
}

func toExprList(values []string, charset, collation string) []ast.ExprNode {
	var exprList []ast.ExprNode
	length := len(values)
	if length > 0 {
		exprList = make([]ast.ExprNode, length)
		for i, str := range values {
			// global相关的，只能生成常量
			exprList[i] = ast.NewValueExpr(str, charset, collation)
		}
	}
	return exprList
}

func newIsolationConditionVisitor(
	mctx mctech.Context,
	charset string, collation string) *isolationConditionVisitor {
	visitor := &isolationConditionVisitor{
		usingParam:          mctx.UsingTenantParam(),
		databaseNameVisitor: newDatabaseNameVisitor(mctx),
		columnModifiedScope: &nodeScope[bool]{items: list.New()},
	}
	result := mctx.ParseResult()
	if result.TenantOmit() {
		visitor.excludes = toExprList(result.Global().Excludes(), charset, collation)
		visitor.includes = toExprList(result.Global().Includes(), charset, collation)
	} else if !visitor.usingParam {
		// 非参数化租户过滤条件，且tenant不为空时
		tenant := result.Tenant().Code()
		if tenant != "" {
			visitor.tenant = ast.NewValueExpr(tenant, charset, collation)
		}
	}

	return visitor
}

func (v *isolationConditionVisitor) enabled() bool {
	return v.usingParam || v.tenant != nil || len(v.excludes) > 0 || len(v.includes) > 0
}

// Enter implements interface Visitor
func (v *isolationConditionVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	v.databaseNameVisitor.Enter(n)

	if v.enabled() {
		switch node := n.(type) {
		case *ast.LoadDataStmt:
			v.enterLoadDataStatement(node)
		case *ast.ImportIntoStmt:
			v.enterImportStatement(node)
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
	if v.enabled() {
		switch node := n.(type) {
		case *ast.DeleteStmt:
			v.leaveDeleteStatement(node)
		case *ast.UpdateStmt:
			v.leaveUpdateStatement(node)
		case *ast.InsertStmt: // include replace / insert .... duplicate
			v.leaveInsertStatement(node)
		case *ast.ImportIntoStmt:
			v.leaveImportIntoStatement(node)
		case *ast.LoadDataStmt:
			v.leaveLoadDataStatement(node)
		case *ast.SelectStmt:
			v.leaveSelectStatement(node)
		case *ast.ValuesExpr: // values ()
		case *ast.SubqueryExpr: // subquery
			v.leaveSubquery(node)
		case *ast.TableSource:
			v.leaveTableSource(node)
		}
	}
	v.databaseNameVisitor.Leave(n)

	return n, true
}

func (v *isolationConditionVisitor) shouldProcess(tableName *ast.TableName) bool {
	dbName := tableName.Schema.L

	sd := v.context
	if dbName == "" {
		dbName = sd.CurrentDB()
	}

	// 只处理global_xxxx的表
	return sd.ParseResult().TenantOmit() || !sd.IsGlobalDb(dbName)
}

func (v *isolationConditionVisitor) enterImportStatement(node *ast.ImportIntoStmt) {
	skip := v.shouldProcess(node.Table)
	if skip {
		return
	}

	modified := v.processImportColumns(node)
	v.columnModifiedScope.Push(modified)
}

func (v *isolationConditionVisitor) leaveImportIntoStatement(_ *ast.ImportIntoStmt) {
	v.columnModifiedScope.Pop()
}

func (v *isolationConditionVisitor) enterLoadDataStatement(node *ast.LoadDataStmt) {
	skip := v.shouldProcess(node.Table)
	if skip {
		return
	}

	modified := v.processLoadColumns(node)
	v.columnModifiedScope.Push(modified)
}

func (v *isolationConditionVisitor) leaveLoadDataStatement(_ *ast.LoadDataStmt) {
	v.columnModifiedScope.Pop()
}

func (v *isolationConditionVisitor) enterInsertStatement(node *ast.InsertStmt) {
	source := node.Table.TableRefs.Left.(*ast.TableSource)
	tableName := source.Source.(*ast.TableName)
	skip := v.shouldProcess(tableName)
	if skip {
		return
	}

	// insert ... 语句有两种格式
	//   1. insert .... columns... values/select ....
	//   2. insert .... set col1=value,col2=value2 ....
	// 第一种用于批量和单条数据，第二种只能用于单条数据。这两种格式不会同时出现在一条语句中

	// 原本v7.1中Setlist表示通过上述第二种方式获取到的赋值列表，在v7.5版InsertStmt类型中Setlist类型改成了bool，仅表示当前语句是否是第二种写法
	// 其赋值语义被拆分到Columns列表和Lists列表中，与原本第一种写法生成语法树对象保持一致。
	// 因此在v7.5版里只需要处理第一种格式就可以了
	modified := v.processInsertColumns(node)
	v.columnModifiedScope.Push(modified)
}

func (v *isolationConditionVisitor) leaveInsertStatement(_ *ast.InsertStmt) {
	v.columnModifiedScope.Pop()
}

func (v *isolationConditionVisitor) enterSubquery(_ *ast.SubqueryExpr) {
	// 子查询不应该受 insert/upsert 的列修改影响
	v.columnModifiedScope.Push(false)
}

func (v *isolationConditionVisitor) leaveSubquery(_ *ast.SubqueryExpr) {
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
	}
	// left, right 都不为nil
	return &ast.BinaryOperationExpr{
		L:  left,
		Op: opcode.LogicAnd,
		R:  right,
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
	dbName, isCteName := v.fetchDbName(table)
	if isCteName {
		// 视图引用，什么也不做
		return nil
	}

	sd := v.context
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
	rt := sd.ParseResult()
	if rt.TenantOmit() {
		var exclude ast.ExprNode
		var include ast.ExprNode
		if len(v.excludes) > 0 {
			exclude = &ast.PatternInExpr{
				Expr: tenantField,
				Not:  true,
				List: v.excludes,
			}
		}

		if len(v.includes) > 0 {
			include = &ast.PatternInExpr{
				Expr: tenantField,
				List: v.includes,
			}
		}

		if exclude == nil {
			condition = include
		} else if include == nil {
			condition = exclude
		} else {
			condition = &ast.BinaryOperationExpr{
				L:  exclude,
				R:  include,
				Op: opcode.LogicAnd,
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
 * 处理load的列，添加tenant字段
 */
func (v *isolationConditionVisitor) processLoadColumns(node *ast.LoadDataStmt) bool {
	columns := node.Columns
	if len(columns) == 0 {
		panic(fmt.Errorf("load语句缺少列定义，无法处理租户信息"))
	}

	for _, i := range node.Columns {
		if i.Name.L == tenantFieldName {
			// 已存在tenant列，忽略
			return false
		}
	}

	// 不存在 tenant 列
	node.ColumnAssignments = append(node.ColumnAssignments, &ast.Assignment{
		Column: &ast.ColumnName{
			Name: model.NewCIStr(tenantFieldName),
		},
		Expr: v.createTenantExpr(),
	})
	return true
}

/**
 * 处理import的列，添加tenant字段
 */
func (v *isolationConditionVisitor) processImportColumns(node *ast.ImportIntoStmt) bool {
	columnExists := false
	for _, i := range node.ColumnsAndUserVars {
		colName := i.ColumnName
		if colName == nil {
			continue
		}
		columnExists = true
		if colName.Name.L == tenantFieldName {
			// 已存在tenant列，忽略
			return false
		}
	}

	if !columnExists {
		panic(fmt.Errorf("import语句缺少列定义，无法处理租户信息"))
	}

	// 补充 tenant 列
	if node.Path != "" {
		// 从文件中导入
		node.ColumnAssignments = append(node.ColumnAssignments, &ast.Assignment{
			Column: &ast.ColumnName{
				Name: model.NewCIStr(tenantFieldName),
			},
			Expr: v.createTenantExpr(),
		})
	} else {
		// 追加tenant列
		node.ColumnsAndUserVars = append(node.ColumnsAndUserVars, &ast.ColumnNameOrUserVar{
			ColumnName: &ast.ColumnName{Name: model.NewCIStr(tenantFieldName)},
		})
	}
	return true
}

/**
 * 处理insert/upsert的列，添加tenant字段
 */
func (v *isolationConditionVisitor) processInsertColumns(node *ast.InsertStmt) bool {
	columns := node.Columns
	if len(columns) == 0 {
		panic(fmt.Errorf("insert/upsert语句缺少列定义，无法处理租户信息"))
	}

	for _, c := range columns {
		if c.Name.L == tenantFieldName {
			// 已存在tenant列，忽略
			return false
		}
	}

	node.Columns = append(node.Columns, &ast.ColumnName{
		Name: model.NewCIStr(tenantFieldName),
	})

	operands := node.Lists
	length := len(operands)
	for i := 0; i < length; i++ {
		operands[i] = append(operands[i], v.createTenantExpr())
	}
	return true
}

func (v *isolationConditionVisitor) createTenantExpr() (expr ast.ValueExpr) {
	if v.usingParam {
		expr = ast.NewParamMarkerExpr(mctech.TenantParamMarkerOffset)
	} else {
		expr = v.tenant
	}
	return expr
}
