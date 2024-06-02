package testkit

import (
	"github.com/pingcap/tidb/mctech"
	_ "github.com/pingcap/tidb/mctech/preps" // 强制调用preps包里的init方法
	"github.com/pingcap/tidb/parser/ast"
)

func (tk *TestKit) onBeforeParseSQL(sql string) (mctech.Context, string, error) {
	return mctech.GetInterceptor().BeforeParseSQL(tk.Session(), sql)
}

func (tk *TestKit) onAfterParseSQL(stmts []ast.StmtNode) (err error) {
	it := mctech.GetInterceptor()
	sctx := tk.Session()
	for _, stmt := range stmts {
		if err = it.AfterParseSQL(sctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (tk *TestKit) onAfterHandleStmt(stmt ast.StmtNode, err error) {
	mctech.GetInterceptor().AfterHandleStmt(tk.Session(), stmt, err)
}
