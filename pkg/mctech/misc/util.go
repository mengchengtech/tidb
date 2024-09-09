package misc

import (
	"sync"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type _miscExtension struct {
}

func (r *_miscExtension) Apply(mctx mctech.Context, node ast.Node) (matched bool, err error) {
	matched = true
	switch stmtNode := node.(type) {
	case *ast.UseStmt:
		stmtNode.DBName, err = r.changeToPhysicalDb(mctx, stmtNode.DBName)
	case *ast.ShowStmt:
		stmtNode.DBName, err = r.changeToPhysicalDb(mctx, stmtNode.DBName)
	case *ast.AnalyzeTableStmt:
		for _, tName := range stmtNode.TableNames {
			var newName string
			newName, err = r.changeToPhysicalDb(mctx, tName.Schema.O)
			newDbName := model.NewCIStr(newName)
			tName.Schema = newDbName
			if tName.DBInfo != nil {
				tName.DBInfo.Name = newDbName
			}
		}
	default:
		matched = false
	}
	return matched, err
}

func (r *_miscExtension) changeToPhysicalDb(
	mctx mctech.Context, oriName string) (dbName string, err error) {
	if dbName, err = mctx.ToPhysicalDbName(oriName); err == nil {
		return dbName, err
	}

	return oriName, err
}

var miscResolver *_miscExtension
var miscResolverInitOne sync.Once

func getMiscExtension() *_miscExtension {
	if miscResolver != nil {
		return miscResolver
	}

	miscResolverInitOne.Do(func() {
		e := &_miscExtension{}
		miscResolver = e
	})
	return miscResolver
}

// ApplyExtension apply misc
func ApplyExtension(mctx mctech.Context, node ast.Node) (matched bool, err error) {
	ext := getMiscExtension()
	return ext.Apply(mctx, node)
}
