package prapared

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/ddl"
	"github.com/pingcap/tidb/mctech/isolation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"golang.org/x/exp/slices"
)

var mctechHintPattern = regexp.MustCompile(`(?i)/*&\s*(\$?[a-z_0-9]+):(.*?)\s*\*/`)

type mctechStatementResolver struct {
	context mctech.Context
	checker *mutexDatabaseChecker
}

func (r *mctechStatementResolver) Context() mctech.Context {
	return r.context
}

/**
 * 预解析sql，解析的结果存到MCTechContext中
 */
func (r *mctechStatementResolver) PrepareSQL(ctx sessionctx.Context, sql string) (string, error) {
	if r.context != nil {
		return "", errors.New("[mctech] PrepareSQL failure, Context exists")
	}

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

	preprocessor := newSQLPreprocessor(sql)
	var preparedSQL string
	result, err := preprocessor.Prepare(ctx, actions, params)
	if err != nil {
		return preparedSQL, err
	}

	preparedSQL = preprocessor.preparedSQL
	r.context = newContext(ctx, result, newDBSelector(result))
	return preparedSQL, nil
}

func (r *mctechStatementResolver) ResolveStmt(
	stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error) {
	dbs, skipped, err = r.rewriteStmt(stmt, charset, collation)
	if err != nil {
		return nil, false, err
	}

	return dbs, skipped, nil
}

func (r *mctechStatementResolver) CheckDB(dbs []string) error {
	return r.checker.Check(r.context, dbs)
}

func (r *mctechStatementResolver) Validate(ctx sessionctx.Context) error {
	prepareResult := r.context.PrepareResult()
	// 执行到此处说明当前语句一定是DML或QUERY
	// sql没有被改写，但是用到了global_xxx数据库，并且没有设置global为true
	if !r.context.SQLRewrited() && r.context.SQLWithGlobalPrefixDB() &&
		!prepareResult.Global() {
		// 检查DML语句和QUERY语句改写状态
		user := currentUser(ctx)
		return fmt.Errorf("用户%s所属的角色无法确定租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */", user)
	}
	return nil
}

func (r *mctechStatementResolver) rewriteStmt(
	stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error) {
	err = ddl.ApplyExtension(r.context, stmt)
	if err != nil {
		return dbs, skipped, err
	}

	dbs, skipped, err = isolation.ApplyExtension(r.context, stmt, charset, collation)
	if skipped || err != nil {
		return dbs, skipped, err
	}

	// 判断sql中是否使用了是否包含'global_xxx'这样的数据库
	hasGlobalDb := slices.IndexFunc(dbs, func(db string) bool {
		return r.context.IsGlobalDb(db)
	}) >= 0

	if !hasGlobalDb {
		return nil, false, nil
	}

	r.context.SetSQLWithGlobalPrefixDB(true)
	result := r.context.PrepareResult()
	if result.Global() {
		// 启用global时，允许跨任意数据库查询
		return nil, false, nil
	}

	// 未启用global,租户code为空，留到后续Validate步骤统一校验
	if result.Tenant() == "" {
		return nil, false, nil
	}

	r.context.SetSQLRewrited(!skipped)
	return dbs, false, nil
}
