package prapared

import (
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// MCTechHandler enhance tidb features
type MCTechHandler struct {
	resolver *mctechStatementResolver
	session  sessionctx.Context
	sql      string
}

// CreateMCTechHandler create MCTechHandler
func CreateMCTechHandler(session sessionctx.Context, sql string) *MCTechHandler {
	return &MCTechHandler{
		resolver: &mctechStatementResolver{
			checker: newMutexDatabaseChecker(),
		},
		session: session,
		sql:     sql,
	}
}

// PrapareSQL prapare sql
func (h *MCTechHandler) PrapareSQL() (sql string, err error) {
	option := mctech.GetOption()
	if !option.TenantEnabled {
		// 禁用租户隔离
		return h.sql, nil
	}

	sql, err = h.resolver.PrepareSQL(h.session, h.sql)
	if err != nil {
		return "", err
	}

	// 改写session上的数据库
	vars := h.session.GetSessionVars()
	currDb := vars.CurrentDB
	currDb, err = h.resolver.Context().ToPhysicalDbName(currDb)
	if err != nil {
		return "", err
	}
	vars.CurrentDB = currDb
	return sql, nil
}

// ApplyAndCheck apply tenant isolation and check db policies
func (h *MCTechHandler) ApplyAndCheck(stmts []ast.StmtNode) (changed bool, err error) {
	option := mctech.GetOption()
	charset, collation := h.session.GetSessionVars().GetCharsetInfo()
	for _, stmt := range stmts {
		var (
			dbs     []string
			skipped = true
		)

		if option.TenantEnabled {
			h.resolver.Context().Reset()
			// 启用租户隔离，改写SQL，添加租户隔离信息
			if dbs, skipped, err = h.resolver.ResolveStmt(stmt, charset, collation); err != nil {
				return false, err
			}
		}

		if !skipped {
			changed = true
		}

		if option.DbCheckerEnabled && len(dbs) > 0 {
			// 启用数据库联合查询规则检查
			if err = h.resolver.CheckDB(dbs); err != nil {
				return changed, err
			}
		}

		if skipped {
			continue
		}

		if option.TenantEnabled {
			// 启用租户隔离，改写SQL，检查租户隔离信息
			err = h.resolver.Validate(h.session)
			if err != nil {
				return changed, err
			}
		}
	}
	return changed, nil
}
