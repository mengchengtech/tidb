package ddl

import (
	"sync"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"golang.org/x/exp/slices"
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

var ddlResolver *_ddlExtension
var ddlResolverInitOne sync.Once

func getDDLExtension() *_ddlExtension {
	if ddlResolver != nil {
		return ddlResolver
	}

	ddlResolverInitOne.Do(func() {
		option := config.GetMCTechConfig()
		e := &_ddlExtension{
			versionEnabled: option.DDL.Version.Enabled,
		}

		if e.versionEnabled {
			e.visitor = newDDLExtensionVisitor(option.DDL.Version.Name)
			matchTexts := append(
				slices.Clone(option.DDL.Version.DbMatches),
			)

			for _, t := range matchTexts {
				if filter, ok := mctech.NewStringFilter(t); ok {
					e.filters = append(e.filters, filter)
				}
			}
		}

		ddlResolver = e
	})
	return ddlResolver
}

// ApplyExtension apply ddl modify
func ApplyExtension(currentDb string, node ast.Node) (matched bool, err error) {
	ext := getDDLExtension()
	return ext.Apply(currentDb, node)
}
