package prapared

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/ddl"
	"github.com/pingcap/tidb/mctech/msic"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

// Handler enhance tidb features
type mctechHandler struct {
	resolver *mctechStatementResolver
}

// PrapareSQL prapare sql
func (h *mctechHandler) PrapareSQL(session sessionctx.Context, rawSql string) (sql string, err error) {
	option := mctech.GetOption()
	if !option.TenantEnabled {
		// 禁用租户隔离
		return rawSql, nil
	}

	sql, err = h.resolver.PrepareSQL(session, rawSql)
	if err != nil {
		return "", err
	}

	mctech.SetContext(session, h.resolver.context)

	// 改写session上的数据库
	vars := session.GetSessionVars()
	currDb := vars.CurrentDB
	currDb, err = h.resolver.Context().ToPhysicalDbName(currDb)
	if err != nil {
		return "", err
	}
	vars.CurrentDB = currDb
	return sql, nil
}

// ApplyAndCheck apply tenant isolation and check db policies
func (h *mctechHandler) ApplyAndCheck(session sessionctx.Context, stmts []ast.StmtNode) (changed bool, err error) {
	option := mctech.GetOption()
	vars := session.GetSessionVars()
	charset, collation := vars.GetCharsetInfo()
	for _, stmt := range stmts {
		var (
			dbs     []string
			skipped = true
		)

		var matched bool
		if matched, err = ddl.ApplyExtension(vars.CurrentDB, stmt); err != nil {
			return false, err
		}

		if !matched {
			if matched, err = msic.ApplyExtension(h.resolver.context, stmt); err != nil {
				return false, err
			}
		}

		// ddl 与dml语句不必重复判断
		if !matched && option.TenantEnabled {
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
			err = h.resolver.Validate(session)
			if err != nil {
				return changed, err
			}
		}
	}
	return changed, nil
}

type handlerFactory struct {
}

// CreateHandler create Handler
func (factory *handlerFactory) CreateHandler() mctech.Handler {
	return &mctechHandler{
		resolver: &mctechStatementResolver{
			checker: getMutexDatabaseChecker(),
		},
	}
}

// CreateHandlerWithContext create Handler
func (factory *handlerFactory) CreateHandlerWithContext(ctx mctech.Context) mctech.Handler {
	return &mctechHandler{
		resolver: &mctechStatementResolver{
			context: ctx,
			checker: getMutexDatabaseChecker(),
		},
	}
}

var factory = &handlerFactory{}

// GetHandlerFactory get MCTechHandler factory
func GetHandlerFactory() mctech.HandlerFactory {
	return factory
}
