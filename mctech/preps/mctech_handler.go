package preps

import (
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/ddl"
	"github.com/pingcap/tidb/mctech/msic"
	"github.com/pingcap/tidb/parser/ast"
)

// CreateHandler create Handler
var handler = &mctechHandler{}

func init() {
	mctech.SetHandler(handler)
}

// Handler enhance tidb features
type mctechHandler struct {
}

// PrepareSQL prepare sql
func (h *mctechHandler) PrepareSQL(mctechCtx mctech.Context, rawSQL string) (sql string, err error) {
	option := mctech.GetOption()
	if !option.TenantEnabled {
		// 禁用租户隔离
		return rawSQL, nil
	}

	var result *mctech.PrepareResult
	sql, result, err = preprocessor.PrepareSQL(mctechCtx, rawSQL)
	if err != nil {
		return "", err
	}

	modifyCtx := mctechCtx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetPrepareResult(result)
	modifyCtx.SetDBSelector(newDBSelector(result))
	// 改写session上的数据库
	vars := mctechCtx.Session().GetSessionVars()
	currDb := vars.CurrentDB
	currDb, err = mctechCtx.ToPhysicalDbName(currDb)
	if err != nil {
		return "", err
	}
	vars.CurrentDB = currDb
	return sql, nil
}

// ApplyAndCheck apply tenant isolation and check db policies
func (h *mctechHandler) ApplyAndCheck(mctechCtx mctech.Context, stmts []ast.StmtNode) (changed bool, err error) {
	option := mctech.GetOption()
	vars := mctechCtx.Session().GetSessionVars()
	charset, collation := vars.GetCharsetInfo()
	preprocessor := preprocessor
	for _, stmt := range stmts {
		var (
			dbs     []string
			skipped = true
			matched bool
		)
		if matched, err = ddl.ApplyExtension(vars.CurrentDB, stmt); err != nil {
			return false, err
		}

		if !matched {
			if matched, err = msic.ApplyExtension(mctechCtx, stmt); err != nil {
				return false, err
			}
		}

		// ddl 与dml语句不必重复判断
		if !matched && option.TenantEnabled {
			modifyCtx := mctechCtx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
			modifyCtx.Reset()
			// 启用租户隔离，改写SQL，添加租户隔离信息
			if dbs, skipped, err = preprocessor.ResolveStmt(mctechCtx, stmt, charset, collation); err != nil {
				return false, err
			}
		}

		if !skipped {
			changed = true
		}

		if option.DbCheckerEnabled && len(dbs) > 0 {
			// 启用数据库联合查询规则检查
			if err = getDatabaseChecker().Check(mctechCtx, dbs); err != nil {
				return changed, err
			}
		}

		if skipped {
			continue
		}

		if option.TenantEnabled {
			// 启用租户隔离，改写SQL，检查租户隔离信息
			err = preprocessor.Validate(mctechCtx)
			if err != nil {
				return changed, err
			}
		}
	}
	return changed, nil
}
