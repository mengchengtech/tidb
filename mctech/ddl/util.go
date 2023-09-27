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
		result, err := filter.Match(db)
		if err != nil {
			return err
		}

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
		option := config.GetOption()
		e := &_ddlExtension{
			versionEnabled: option.DDLVersionColumnEnabled,
		}

		if e.versionEnabled {
			e.visitor = newDDLExtensionVisitor(option.DDLVersionColumnName)
			matchTexts := append(
				slices.Clone(option.DDLVersionDbMatches),
				mctech.PrefixFilterPattern(mctech.DbGlobalPrefix),
				mctech.PrefixFilterPattern(mctech.DbAssetPrefix),
				mctech.PrefixFilterPattern(mctech.DbPublicPrefix),
				mctech.SuffixFilterPattern(mctech.DbCustomSuffix),
			)

			e.filters = make([]mctech.Filter, len(matchTexts))
			for i, t := range matchTexts {
				e.filters[i] = mctech.NewStringFilter(t)
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
