package preps

import (
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/ddl"
	"github.com/pingcap/tidb/mctech/msic"
	"github.com/pingcap/tidb/parser/ast"
)

func init() {
	mctech.SetHandler(&mctechHandler{})
}

// Handler enhance tidb features
type mctechHandler struct{}

// PrepareSQL prepare sql
func (h *mctechHandler) PrepareSQL(mctx mctech.Context, rawSQL string) (sql string, err error) {
	option := config.GetMCTechConfig()
	if !option.Tenant.Enabled {
		// 禁用租户隔离
		return rawSQL, nil
	}

	var result *mctech.PrepareResult
	sql, result, err = preprocessor.PrepareSQL(mctx, rawSQL)
	if err != nil {
		return "", err
	}

	modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetPrepareResult(result)
	modifyCtx.SetDBSelector(newDBSelector(result))
	// 改写session上的数据库
	vars := mctx.Session().GetSessionVars()
	currDb := vars.CurrentDB
	currDb, err = mctx.ToPhysicalDbName(currDb)
	if err != nil {
		return "", err
	}
	vars.CurrentDB = currDb
	return sql, nil
}

// ApplyAndCheck apply tenant isolation and check db policies
func (h *mctechHandler) ApplyAndCheck(mctx mctech.Context, stmt ast.StmtNode) (bool, error) {
	option := config.GetMCTechConfig()
	vars := mctx.Session().GetSessionVars()
	charset, collation := vars.GetCharsetInfo()
	preprocessor := preprocessor

	// 是否改写过sql
	var changed bool
	if isDDL, err := ddl.ApplyExtension(vars.CurrentDB, stmt); err != nil || isDDL {
		// 有错误 或者是 ddl语句，不再执行后续处理逻辑
		return false, err
	}

	// 返回值skip: 是否属于特殊的sql（use/show 等）
	if isMsic, err := msic.ApplyExtension(mctx, stmt); err != nil || isMsic {
		// 有错误 或者是 misc语句，不再执行后续处理逻辑
		return false, err
	}

	var (
		dbs []string // sql中用到的数据库
		err error
	)
	// ddl 与dml语句不必重复判断
	if option.Tenant.Enabled {
		modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
		modifyCtx.Reset()

		skipped := true // 是否需要跳过后续处理
		// 启用租户隔离，改写SQL，添加租户隔离信息
		if dbs, skipped, err = preprocessor.ResolveStmt(mctx, stmt, charset, collation); err != nil {
			return false, err
		}
		mctx.SetDbs(stmt, dbs)
		if !skipped {
			changed = true
		}

		if option.DbChecker.Enabled && len(dbs) > 0 {
			// 启用数据库联合查询规则检查
			if err = getDatabaseChecker().Check(mctx, stmt, dbs); err != nil {
				return changed, err
			}
		}

		if skipped {
			return changed, nil
		}

		// 启用租户隔离，改写SQL，检查租户隔离信息
		err = preprocessor.Validate(mctx)
		if err != nil {
			return changed, err
		}
	}
	return changed, nil
}
