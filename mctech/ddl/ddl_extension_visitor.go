package ddl

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	f "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"golang.org/x/exp/slices"
)

func compareDefaultValue(expr ast.ExprNode) (match bool) {
	if fcallExpr, ok := expr.(*ast.FuncCallExpr); ok {
		// 函数调用名，函数参数都相同
		if fcallExpr.FnName.L == "mctech_sequence" && len(fcallExpr.Args) == 0 {
			match = true
		} else {
			match = false
		}
	} else {
		match = false
	}
	return match
}

type ddlExtensionVisitor struct {
	versionColumnName string
	versionColumn     *ast.ColumnDef
}

func newDDLExtensionVisitor(columnName string) *ddlExtensionVisitor {
	funcExpr := &ast.FuncCallExpr{
		Tp:     ast.FuncCallExprTypeGeneric,
		Schema: model.CIStr{},
		FnName: model.NewCIStr("MCTECH_SEQUENCE"),
		Args:   []ast.ExprNode{},
	}

	versionDef := &ast.ColumnDef{
		Name: &ast.ColumnName{Name: model.NewCIStr(columnName)},
		Tp:   types.NewFieldType(mysql.TypeLonglong),
		Options: []*ast.ColumnOption{
			{Tp: ast.ColumnOptionNotNull},
			{Tp: ast.ColumnOptionDefaultValue, Expr: funcExpr},
			{Tp: ast.ColumnOptionOnUpdate, Expr: funcExpr},
		},
	}
	return &ddlExtensionVisitor{
		versionColumn: versionDef,
	}
}

func (v *ddlExtensionVisitor) isVersionColumn(colName *ast.ColumnName) bool {
	return colName.Name.L == v.versionColumnName
}

// Enter implements interface Visitor
func (v *ddlExtensionVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	var err error
	switch node := n.(type) {
	case *ast.CreateTableStmt:
		err = v.addVersionColumn(node)
	case *ast.AlterTableStmt:
		err = v.checkAlterTableSpecs(node)
	}
	if err != nil {
		panic(err)
	}
	return n, false
}

// Leave implements interface Visitor
func (v *ddlExtensionVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, true
}

func (v *ddlExtensionVisitor) isVersionColumnSpec(tp *types.FieldType, options []*ast.ColumnOption) bool {
	if tp.GetType() != v.versionColumn.Tp.GetType() {
		return false
	}

	var match bool
	count := 0
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionComment:
			match = true
		case ast.ColumnOptionNotNull:
			count += 1
			match = true
		case ast.ColumnOptionOnUpdate:
			count += 1
			match = compareDefaultValue(opt.Expr)
		case ast.ColumnOptionDefaultValue:
			match = compareDefaultValue(opt.Expr)
			if match {
				count += 1
			}
		default:
			match = false
		}

		if !match {
			break
		}
	}

	return match && count == len(v.versionColumn.Options)
}

func (v *ddlExtensionVisitor) checkAlterTableSpecs(node *ast.AlterTableStmt) (err error) {
	for _, spec := range node.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns, ast.AlterTableModifyColumn:
			index := slices.IndexFunc(spec.NewColumns, func(def *ast.ColumnDef) bool {
				return v.isVersionColumn(def.Name)
			})
			if index >= 0 {
				def := spec.NewColumns[index]
				if !v.isVersionColumnSpec(def.Tp, def.Options) {
					var sb strings.Builder
					err = v.versionColumn.Restore(f.NewRestoreCtx(f.RestoreKeyWordUppercase, &sb))
					if err == nil {
						err = fmt.Errorf("'%s' 字段定义不正确，允许的定义为 -> %s", v.versionColumnName, sb.String())
					}
				}
			}
		case ast.AlterTableRenameColumn:
			if v.isVersionColumn(spec.OldColumnName) {
				err = fmt.Errorf("'%s' 字段不支持修改名称", v.versionColumnName)
			} else if v.isVersionColumn(spec.NewColumnName) {
				err = fmt.Errorf("不支持把其它字段名称修改为'%s'", v.versionColumnName)
			}
		case ast.AlterTableDropColumn:
			if v.isVersionColumn(spec.OldColumnName) {
				err = fmt.Errorf("'%s' 字段不允许删除", v.versionColumnName)
			}
		case ast.AlterTableChangeColumn:
			def := spec.NewColumns[0] // 只会有一个列定义
			if v.isVersionColumn(spec.OldColumnName) {
				if !v.isVersionColumn(def.Name) {
					err = fmt.Errorf("'%s' 字段不支持修改名称", v.versionColumnName)
				} else if !v.isVersionColumnSpec(def.Tp, def.Options) {
					var sb strings.Builder
					err = v.versionColumn.Restore(f.NewRestoreCtx(f.RestoreKeyWordUppercase, &sb))
					if err == nil {
						err = fmt.Errorf("'%s' 字段定义不正确，允许的定义为 -> %s", v.versionColumnName, sb.String())
					}
				}
			} else if v.isVersionColumn(def.Name) {
				err = fmt.Errorf("不支持把其它字段名称修改为'%s'", v.versionColumnName)
			}
		case ast.AlterTableAlterColumn:
			// alter column 子语句只支持修改一列的默认值
			def := spec.NewColumns[0]
			if v.isVersionColumn(def.Name) {
				if len(def.Options) == 0 {
					err = fmt.Errorf("'%s' 字段不允删除默认值", v.versionColumnName)
				} else {
					opt := def.Options[0]
					// 此处只能是修改默认值表达式
					if !compareDefaultValue(opt.Expr) {
						err = fmt.Errorf("'%s' 字段不允修改默认值", v.versionColumnName)
					}
				}
			}
		}
	}
	return err
}

func (v *ddlExtensionVisitor) addVersionColumn(node *ast.CreateTableStmt) error {
	index := slices.IndexFunc(node.Cols, func(n *ast.ColumnDef) bool {
		return n.Name.Name.L == v.versionColumnName
	})

	if index >= 0 {
		return fmt.Errorf("'__version' is reserved column name")
	}

	node.Cols = append(node.Cols, v.versionColumn)
	return nil
}

func ApplyDDLExtension(node ast.Node) (err error) {
	option := mctech.GetOption()
	if !option.DDLVersionColumnEnabled {
		return
	}

	v := newDDLExtensionVisitor(option.DDLVersionColumnName)
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	node.Accept(v)
	return err
}
