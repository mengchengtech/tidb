package msic

import (
	"sync"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
)

type _msicExtension struct {
}

func (r *_msicExtension) Apply(mctx mctech.Context, node ast.Node) (matched bool, err error) {
	matched = true
	switch stmtNode := node.(type) {
	case *ast.UseStmt:
		stmtNode.DBName, err = r.changeToPhysicalDb(mctx, stmtNode.DBName)
	case *ast.ShowStmt:
		stmtNode.DBName, err = r.changeToPhysicalDb(mctx, stmtNode.DBName)
	default:
		matched = false
	}
	return matched, err
}

func (r *_msicExtension) changeToPhysicalDb(
	mctx mctech.Context, oriName string) (dbName string, err error) {
	if dbName, err = mctx.ToPhysicalDbName(oriName); err == nil {
		return dbName, err
	}

	return oriName, err
}

var msicResolver *_msicExtension
var msicResolverInitOne sync.Once

func getMsicExtension() *_msicExtension {
	if msicResolver != nil {
		return msicResolver
	}

	msicResolverInitOne.Do(func() {
		e := &_msicExtension{}
		msicResolver = e
	})
	return msicResolver
}

// ApplyExtension apply msic
func ApplyExtension(mctx mctech.Context, node ast.Node) (matched bool, err error) {
	ext := getMsicExtension()
	return ext.Apply(mctx, node)
}
