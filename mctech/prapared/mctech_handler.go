package prapared

import (
	"github.com/pingcap/tidb/mctech"
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
	option := mctech.GetOption()
	if !option.Tenant_Enabled {
		// 禁用租户隔离
		return h.sql, nil
	}

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

func (h *MctechHandler) ResolveAndValidate(stmts []ast.StmtNode) (changed bool, err error) {
	option := mctech.GetOption()
	charset, collation := h.session.GetSessionVars().GetCharsetInfo()
	for _, stmt := range stmts {
		var (
			dbs     []string
			skipped = true
		)

		if option.Tenant_Enabled {
			h.resolver.Context().Reset()
			// 启用租户隔离，改写SQL，添加租户隔离信息
			if dbs, skipped, err = h.resolver.ResolveStmt(stmt, charset, collation); err != nil {
				return false, err
			}
		}

		if !skipped {
			changed = true
		}

		if option.DbChecker_Enabled && len(dbs) > 0 {
			// 启用数据库联合查询规则检查
			if err = h.resolver.CheckDB(dbs); err != nil {
				return changed, err
			}
		}

		if skipped {
			continue
		}

		if option.Tenant_Enabled {
			// 启用租户隔离，改写SQL，检查租户隔离信息
			err = h.resolver.Validate(h.session)
			if err != nil {
				return changed, err
			}
		}
	}
	return changed, nil
}
