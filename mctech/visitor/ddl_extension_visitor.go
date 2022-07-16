package visitor

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser/ast"
	f "github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"golang.org/x/exp/slices"
)

const versionColumnName = "__version"

var versionDef = &ast.ColumnDef{
	Name: &ast.ColumnName{Name: model.NewCIStr(versionColumnName)},
	Tp:   types.NewFieldType(mysql.TypeLonglong),
	Options: []*ast.ColumnOption{
		{Tp: ast.ColumnOptionNotNull},
		{
			Tp: ast.ColumnOptionDefaultValue,
			Expr: &ast.FuncCallExpr{
				Tp:     ast.FuncCallExprTypeGeneric,
				Schema: model.CIStr{},
				FnName: model.NewCIStr("MCTECH_SEQUENCE"),
				Args:   []ast.ExprNode{},
			},
		},
		{Tp: ast.ColumnOptionOnUpdate,
			Expr: &ast.FuncCallExpr{
				Tp:     ast.FuncCallExprTypeGeneric,
				Schema: model.CIStr{},
				FnName: model.NewCIStr("MCTECH_SEQUENCE"),
				Args:   []ast.ExprNode{},
			},
		},
	},
}

func isVersionColumn(colName *ast.ColumnName) bool {
	return colName.Name.L == versionColumnName
}

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

func isVersionColumnSpec(tp *types.FieldType, options []*ast.ColumnOption) bool {
	if tp.GetType() != versionDef.Tp.GetType() {
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

	return match && count == len(versionDef.Options)
}

type ddlExtensionVisitor struct {
	versionColumn *ast.ColumnDef
}

func newDDLExtensionVisitor() *ddlExtensionVisitor {
	return &ddlExtensionVisitor{versionColumn: versionDef}
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

func (v *ddlExtensionVisitor) checkAlterTableSpecs(node *ast.AlterTableStmt) (err error) {
	for _, spec := range node.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns, ast.AlterTableModifyColumn:
			index := slices.IndexFunc(spec.NewColumns, func(def *ast.ColumnDef) bool {
				return isVersionColumn(def.Name)
			})
			if index >= 0 {
				def := spec.NewColumns[index]
				if !isVersionColumnSpec(def.Tp, def.Options) {
					var sb strings.Builder
					err = versionDef.Restore(f.NewRestoreCtx(f.RestoreKeyWordUppercase, &sb))
					if err == nil {
						err = fmt.Errorf("'%s' 字段定义不正确，允许的定义为 -> %s", versionColumnName, sb.String())
					}
				}
			}
		case ast.AlterTableRenameColumn:
			if isVersionColumn(spec.OldColumnName) {
				err = fmt.Errorf("'%s' 字段不支持修改名称", versionColumnName)
			} else if isVersionColumn(spec.NewColumnName) {
				err = fmt.Errorf("不支持把其它字段名称修改为'%s'", versionColumnName)
			}
		case ast.AlterTableDropColumn:
			if isVersionColumn(spec.OldColumnName) {
				err = fmt.Errorf("'%s' 字段不允许删除", versionColumnName)
			}
		case ast.AlterTableChangeColumn:
			def := spec.NewColumns[0] // 只会有一个列定义
			if isVersionColumn(spec.OldColumnName) {
				if !isVersionColumn(def.Name) {
					err = fmt.Errorf("'%s' 字段不支持修改名称", versionColumnName)
				} else if !isVersionColumnSpec(def.Tp, def.Options) {
					var sb strings.Builder
					err = versionDef.Restore(f.NewRestoreCtx(f.RestoreKeyWordUppercase, &sb))
					if err == nil {
						err = fmt.Errorf("'%s' 字段定义不正确，允许的定义为 -> %s", versionColumnName, sb.String())
					}
				}
			} else if isVersionColumn(def.Name) {
				err = fmt.Errorf("不支持把其它字段名称修改为'%s'", versionColumnName)
			}
		case ast.AlterTableAlterColumn:
			// alter column 子语句只支持修改一列的默认值
			def := spec.NewColumns[0]
			if isVersionColumn(def.Name) {
				if len(def.Options) == 0 {
					err = fmt.Errorf("'%s' 字段不允删除默认值", versionColumnName)
				} else {
					opt := def.Options[0]
					// 此处只能是修改默认值表达式
					if !compareDefaultValue(opt.Expr) {
						err = fmt.Errorf("'%s' 字段不允修改默认值", versionColumnName)
					}
				}
			}
		}
	}
	return err
}

func (v *ddlExtensionVisitor) addVersionColumn(node *ast.CreateTableStmt) error {
	index := slices.IndexFunc(node.Cols, func(n *ast.ColumnDef) bool {
		return n.Name.Name.L == versionColumnName
	})

	if index >= 0 {
		return fmt.Errorf("'__version' is reserved column name")
	}

	node.Cols = append(node.Cols, v.versionColumn)
	return nil
}
