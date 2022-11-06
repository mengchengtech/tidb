package msic

import (
	"sync"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
)

type _msicExtension struct {
}

func (r *_msicExtension) Apply(mctechCtx mctech.Context, node ast.Node) (matched bool, err error) {
	matched = true
	switch stmtNode := node.(type) {
	case *ast.UseStmt:
		stmtNode.DBName, err = r.changeToPhysicalDb(mctechCtx, stmtNode.DBName)
	case *ast.ShowStmt:
		stmtNode.DBName, err = r.changeToPhysicalDb(mctechCtx, stmtNode.DBName)
	default:
		matched = false
	}
	return matched, err
}

func (r *_msicExtension) changeToPhysicalDb(
	mctechCtx mctech.Context, oriName string) (dbName string, err error) {
	if dbName, err = mctechCtx.ToPhysicalDbName(oriName); err == nil {
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
func ApplyExtension(mctechCtx mctech.Context, node ast.Node) (matched bool, err error) {
	ext := getMsicExtension()
	return ext.Apply(mctechCtx, node)
}
