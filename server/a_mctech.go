// add by zhangbing

package server

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/prepare"
	_ "github.com/pingcap/tidb/mctech/preps" // 强制初始化preps
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

// onBeforeParseSQL sql语法解析前 执行的方法
func (cc *clientConn) onBeforeParseSQL(sql string) (mctech.Context, string, error) {
	return mctech.GetInterceptor().BeforeParseSQL(cc.getCtx(), sql)
}

// onAfterParseSQL 当sql语法解析成功后 执行的方法
func (cc *clientConn) onAfterParseSQL(stmt ast.StmtNode) (err error) {
	return mctech.GetInterceptor().AfterParseSQL(cc.getCtx(), stmt)
}

func (cc *clientConn) onExtensionBinaryPrepareEnd(stmt PreparedStatement, sql string, err error) {
	var stmtID uint32
	if stmt != nil {
		stmtID = uint32(stmt.ID())
	}
	prepared, errNotFound := cc.ctx.GetSessionVars().GetPreparedStmtByID(stmtID)
	it := mctech.GetInterceptor()
	if err == nil {
		prepareStmt := &prepare.BinaryPrepareStmt{
			PrepareStmt: ast.PrepareStmt{SQLText: sql},
			PrepStmt:    prepared,
		}
		it.AfterHandleStmt(cc.getCtx(), prepareStmt, nil)
	} else if errNotFound == nil {
		// stmtNode 不存在，sql 解析失败
		it.ParseSQLFailed(cc.getCtx(), sql, err)
	}
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
