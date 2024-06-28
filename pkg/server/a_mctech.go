// add by zhangbing

package server

import (
	"github.com/pingcap/tidb/pkg/mctech"
	_ "github.com/pingcap/tidb/pkg/mctech/preps" // 强制初始化preps
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

func (cc *clientConn) onBeforeParseSQL(sql string) (mctech.Context, string, error) {
	return mctech.GetInterceptor().BeforeParseSQL(cc.getCtx(), sql)
}

func (cc *clientConn) onAfterParseSQL(stmts []ast.StmtNode) (err error) {
	it := mctech.GetInterceptor()
	sctx := cc.getCtx()
	for _, stmt := range stmts {
		if err = it.AfterParseSQL(sctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

type mctechStmtEventInfo struct {
	stmtEventInfo
	sctx sessionctx.Context
}

func (e *mctechStmtEventInfo) SCtx() sessionctx.Context {
	return e.sctx
}

var (
	_ mctech.SessionStmtEventInfo = &mctechStmtEventInfo{}
)
