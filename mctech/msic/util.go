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
		err = r.changeToPhysicalDb(mctechCtx, stmtNode)
	default:
		matched = false
	}
	return matched, err
}

func (r *_msicExtension) changeToPhysicalDb(context mctech.Context, n *ast.UseStmt) (err error) {
	var dbName string
	if dbName, err = context.ToPhysicalDbName(n.DBName); err == nil {
		n.DBName = dbName
	}

	return err
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
