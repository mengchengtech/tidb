package prapared

import (
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

type MctechHandler struct {
	resolver *mctechStatementResolver
	session  sessionctx.Context
	sql      string
}

func CreateMctechHandler(session sessionctx.Context, sql string) *MctechHandler {
	return &MctechHandler{
		resolver: &mctechStatementResolver{
			checker: NewMutexDatabaseChecker(),
		},
		session: session,
		sql:     sql,
	}
}

func (h *MctechHandler) PrapareSql() (sql string, err error) {
	sql, err = h.resolver.PrepareSql(h.session, h.sql)
	if err != nil {
		return "", err
	}

	// 改写session上的数据库
	vars := h.session.GetSessionVars()
	currDB := vars.CurrentDB
	currDB, err = h.resolver.Context().ToPhysicalDbName(currDB)
	if err != nil {
		return "", err
	}
	vars.CurrentDB = currDB
	return sql, nil
}

func (h *MctechHandler) ResolveAndValidate(stmts []ast.StmtNode) error {
	charset, collation := h.session.GetSessionVars().GetCharsetInfo()
	for _, stmt := range stmts {
		h.resolver.Context().Reset()
		skipped, err := h.resolver.ResolveStmt(stmt, charset, collation)
		if err != nil {
			return err
		}
		if skipped {
			continue
		}

		err = h.resolver.Validate(h.session)
		if err != nil {
			return err
		}
	}
	return nil
}
