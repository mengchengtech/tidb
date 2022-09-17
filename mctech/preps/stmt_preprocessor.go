package preps

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/isolation"
	"github.com/pingcap/tidb/parser/ast"
	"golang.org/x/exp/slices"
)

var mctechHintPattern = regexp.MustCompile(`(?i)/*&\s*(\$?[a-z_0-9]+):(.*?)\s*\*/`)

// StatementPreprocessor interface
type StatementPreprocessor interface {
	PrepareSQL(mctechCtx mctech.Context, sql string) (string, *mctech.PrepareResult, error)
	ResolveStmt(mctechCtx mctech.Context,
		stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error)
	Validate(mctechCtx mctech.Context) error
}

type mctechStatementPreprocessor struct {
}

type actionInfo struct {
	name string
	args string
}

/**
 * 预解析sql，解析的结果存到MCTechContext中
 */
func (r *mctechStatementPreprocessor) PrepareSQL(
	mctechCtx mctech.Context, sql string) (string, *mctech.PrepareResult, error) {
	if mctechCtx.PrepareResult() != nil {
		return "", nil, errors.New("[mctech] PrepareSQL failure, Context exists")
	}

	params := map[string]any{}
	actions := []*actionInfo{}

	matches := mctechHintPattern.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		name := match[1]
		value := match[2]

		if strings.HasPrefix(name, "$") {
			// action 去掉'$'前缀
			actionName := name[1:]
			actions = append(actions, &actionInfo{actionName, value})
		} else {
			// param 去掉两端的单引号
			if value[0] == '\'' && value[len(value)-1] == '\'' {
				value = value[1 : len(value)-1]
			}
			if val, ok := params[name]; ok {
				if val != value {
					return "", nil, fmt.Errorf("多个 %s hint包含不同的值: %s <=> %s",
						name, val, value)
				}
			}
			params[name] = value
		}
	}

	preprocessor := newSQLPreprocessor(sql)
	var preparedSQL string
	result, err := preprocessor.Prepare(mctechCtx, actions, params)
	if err != nil {
		return preparedSQL, nil, err
	}

	preparedSQL = preprocessor.preparedSQL
	return preparedSQL, result, nil
}

func (r *mctechStatementPreprocessor) ResolveStmt(mctechCtx mctech.Context,
	stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error) {
	dbs, skipped, err = r.rewriteStmt(mctechCtx, stmt, charset, collation)
	if err != nil {
		return nil, false, err
	}

	return dbs, skipped, nil
}

func (r *mctechStatementPreprocessor) Validate(mctechCtx mctech.Context) error {
	prepareResult := mctechCtx.PrepareResult()
	// 执行到此处说明当前语句一定是DML或QUERY
	// sql没有被改写，但是用到了global_xxx数据库，并且没有设置global为true
	if !mctechCtx.SQLRewrited() && mctechCtx.SQLHasGlobalDB() &&
		!prepareResult.Global() {
		// 检查DML语句和QUERY语句改写状态
		user := currentUser(mctechCtx.Session())
		return fmt.Errorf("当前用户%s无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */", user)
	}
	return nil
}

func (r *mctechStatementPreprocessor) rewriteStmt(mctechCtx mctech.Context,
	stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error) {
	dbs, skipped, err = isolation.ApplyExtension(mctechCtx, stmt, charset, collation)
	if skipped || err != nil {
		return dbs, skipped, err
	}

	// 判断sql中是否使用了是否包含'global_xxx'这样的数据库
	hasGlobalDb := slices.IndexFunc(dbs, func(db string) bool {
		return mctechCtx.IsGlobalDb(db)
	}) >= 0

	if !hasGlobalDb {
		return nil, false, nil
	}

	modifyCtx := mctechCtx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetSQLHasGlobalDB(true)
	result := mctechCtx.PrepareResult()
	if result.Global() {
		// 启用global时，允许跨任意数据库查询
		return nil, false, nil
	}

	// 未启用global,租户code为空，留到后续Validate步骤统一校验
	if result.Tenant() == "" && !mctechCtx.UsingTenantParam() {
		return nil, false, nil
	}

	modifyCtx.SetSQLRewrited(!skipped)
	return dbs, false, nil
}

var preprocessor StatementPreprocessor = &mctechStatementPreprocessor{}
