package ddl

import (
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type _ddlExtension struct {
	versionEnabled bool
	filters        []mctech.Filter
	visitor        ast.Visitor
}

func (r *_ddlExtension) Apply(currentDb string, node ast.Node) (matched bool, err error) {
	if !r.versionEnabled {
		return false, nil
	}

	matched = true
	switch stmtNode := node.(type) {
	case *ast.CreateTableStmt:
		err = r.doApply(currentDb, stmtNode.Table, stmtNode)
	case *ast.AlterTableStmt:
		err = r.doApply(currentDb, stmtNode.Table, stmtNode)
	default:
		err = nil
		matched = false
	}
	return matched, err
}

func (r *_ddlExtension) doApply(currentDb string, table *ast.TableName, node ast.Node) (err error) {
	db := table.Schema.L
	if db == "" {
		db = currentDb
		if db == "" {
			db = "test"
		}
	}

	var matched bool
	for _, filter := range r.filters {
		result := filter.Match(db)
		if result {
			matched = result
			break
		}
	}

	if !matched {
		return nil
	}

	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()
	node.Accept(r.visitor)
	return err
}

var ddlVisitor *_ddlExtension
var ddlVisitorInitOne sync.Once

func getDDLExtension() *_ddlExtension {
	if ddlVisitor != nil {
		return ddlVisitor
	}

	ddlVisitorInitOne.Do(func() {
		option := config.GetMCTechConfig()
		e := &_ddlExtension{
			versionEnabled: option.DDL.Version.Enabled,
		}

		if e.versionEnabled {
			e.visitor = newDDLExtensionVisitor(option.DDL.Version.Name)
			matchTexts := slices.Clone(option.DDL.Version.DbMatches)

			for _, t := range matchTexts {
				if filter, ok := mctech.NewStringFilter(t); ok {
					e.filters = append(e.filters, filter)
				}
			}
		}

		ddlVisitor = e
	})
	return ddlVisitor
}

// ApplyExtension apply ddl modify
func ApplyExtension(currentDb string, node ast.Node) (matched bool, err error) {
	ext := getDDLExtension()
	return ext.Apply(currentDb, node)
}
