package prapared

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/tenant"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

var mctechHintPattern = regexp.MustCompile(`(?i)/*&\s*(\$?[a-z_0-9]+):(.*?)\s*\*/`)

type mctechStatementResolver struct {
	context mctech.MCTechContext
	checker *MutexDatabaseChecker
}

func NewStatementResolver() mctech.StatementResolver {
	return &mctechStatementResolver{
		checker: NewMutexDatabaseChecker(),
	}
}

func (r *mctechStatementResolver) Context() mctech.MCTechContext {
	return r.context
}

/**
 * 预解析sql，解析的结果存到MCTechContext中
 */
func (r *mctechStatementResolver) PrepareSql(ctx sessionctx.Context, sql string) (string, error) {
	params := map[string]any{}
	actions := map[string]string{}

	matches := mctechHintPattern.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		name := match[1]
		value := match[2]

		if strings.HasPrefix(name, "$") {
			// action 去掉'$'前缀
			actionName := name[1:]
			actions[actionName] = value
		} else {
			// param 去掉两端的单引号
			if value[0] == '\'' && value[len(value)-1] == '\'' {
				value = value[1 : len(value)-1]
			}
			params[name] = value
		}
	}

	preprocessor := NewSqlPreprocessor(sql)
	var preparedSql string
	result, err := preprocessor.Prepare(ctx, actions, params)
	if err != nil {
		return preparedSql, err
	}

	preparedSql = preprocessor.preparedSql
	r.context = NewMCTechContext(ctx, result, newDBSelector(result))
	return preparedSql, nil
}

func (r *mctechStatementResolver) ResolveStmt(stmt ast.Node, charset string, collation string) error {
	dbs, err := r.rewriteStmt(stmt, charset, collation)
	if err != nil {
		return err
	}
	return r.checker.Check(r.context, dbs)
}

func (r *mctechStatementResolver) Validate(ctx sessionctx.Context) error {
	resolveResult := r.context.ResolveResult()
	// 执行到此处说明当前语句一定是DML或QUERY
	// sql没有被改写，但是用到了global_xxx数据库，并且没有设置global为true
	if !r.context.SqlRewrited() && r.context.SqlWithGlobalPrefixDB() &&
		!resolveResult.Global() {
		// 检查DML语句和QUERY语句改写状态
		user := currentUser(ctx)
		return fmt.Errorf("用户%s所属的角色无法确定租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */", user)
	}
	return nil
}

func (r *mctechStatementResolver) rewriteStmt(stmt ast.Node, charset string, collation string) ([]string, error) {
	dbs, skipped, err := tenant.ApplyTenantIsolation(r.context, stmt, charset, collation)
	if skipped || err != nil {
		return dbs, err
	}

	// 此处不能从tableCache.dbs里获取db的列表，应该从tableCache.tables获取
	// 因为dbs中包含了currentDb信息，而tables里只包含了从sql解析中获取到的表以及对应的数据库信息
	// 当currentDb与sql中用到的所有表所属的库都不一样时，使用dbs就会出现判断错误
	hasGlobalDb := false
	for _, db := range dbs {
		if r.context.IsGlobalDb(db) {
			hasGlobalDb = true
			break
		}
	}

	if !hasGlobalDb {
		return nil, nil
	}

	r.context.SetSqlWithGlobalPrefixDB(true)
	result := r.context.ResolveResult()
	if result.Global() {
		// 启用了global且没有需要排除的租户
		if len(result.Excludes()) == 0 {
			if !hasGlobalDb {
				return nil, nil
			}
		}
	} else {
		// 未启用global,租户code为空，留到后续Validate步骤统一校验
		if result.Tenant() == "" {
			return nil, nil
		}
	}

	r.context.SetSqlRewrited(!skipped)
	return dbs, nil
}
