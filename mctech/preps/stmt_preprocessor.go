package preps

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/isolation"
	"github.com/pingcap/tidb/parser/ast"
	"golang.org/x/exp/slices"
)

var mctechHintPattern = regexp.MustCompile(`(?i)/\*&\s*(\$?[a-z_0-9]+)[:|]\s*(.*?)\s*\*/`)

// StatementPreprocessor interface
type StatementPreprocessor interface {
	PrepareSQL(mctx mctech.Context, sql string) (string, *mctech.PrepareResult, error)
	ResolveStmt(mctx mctech.Context,
		stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error)
	Validate(mctx mctech.Context) error
}

type mctechStatementPreprocessor struct {
}

// ActionInfo 自定义指令描述接口
type ActionInfo interface {
	Name() string
	Args() string
}

type actionInfo struct {
	name string
	args string
}

func (i *actionInfo) Name() string { return i.name }
func (i *actionInfo) Args() string { return i.args }

/**
 * 预解析sql，解析的结果存到MCTechContext中
 */
func (r *mctechStatementPreprocessor) PrepareSQL(
	mctx mctech.Context, sql string) (string, *mctech.PrepareResult, error) {
	if mctx.PrepareResult() != nil {
		return "", nil, errors.New("[mctech] PrepareSQL failure, Context exists")
	}

	params := map[string]any{}
	actions := []ActionInfo{}

	matches := mctechHintPattern.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		name := match[1]
		value := match[2]

		if strings.HasPrefix(name, "$") {
			// action 去掉'$'前缀
			actionName := name[1:]
			actions = append(actions, &actionInfo{actionName, value})
		} else {
			valueLength := len(value)
			// param 去掉两端的单引号
			if valueLength > 0 {
				quotedPrefix := value[0] == '\''
				quotedSuffix := value[valueLength-1] == '\''
				switch {
				case
					valueLength == 1 && quotedPrefix, // "'"
					quotedPrefix && !quotedSuffix,    // "'foo"
					!quotedPrefix && quotedSuffix:    // "bar'"
					return "", nil, fmt.Errorf("\"%s\" hint 值格式不正确 -> %s", name, value)
				case quotedPrefix && quotedSuffix:
					value = value[1 : valueLength-1]
					valueLength = len(value)
				}
			}
			if valueLength > 0 && (value[0] == ' ' || value[valueLength-1] == ' ') {
				value = strings.TrimSpace(value)
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
	comments := GetCustomCommentFromSQL(sql)
	result, err := preprocessor.Prepare(mctx, actions, comments, params)
	if err != nil {
		return preparedSQL, nil, err
	}

	preparedSQL = preprocessor.preparedSQL
	return preparedSQL, result, nil
}

func (r *mctechStatementPreprocessor) ResolveStmt(mctx mctech.Context,
	stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error) {
	dbs, skipped, err = r.rewriteStmt(mctx, stmt, charset, collation)
	if err != nil {
		return nil, false, err
	}

	dbs = normalize(dbs)
	return dbs, skipped, nil
}

func (r *mctechStatementPreprocessor) Validate(mctx mctech.Context) error {
	prepareResult := mctx.PrepareResult()
	// 执行到此处说明当前语句一定是DML或QUERY
	// sql没有被改写，但是用到了global_xxx数据库，并且没有设置global为true
	if !mctx.SQLRewrited() && mctx.SQLHasGlobalDB() &&
		!prepareResult.Global() {
		// 检查DML语句和QUERY语句改写状态
		user := currentUser(mctx.Session())
		return fmt.Errorf("当前用户%s无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */", user)
	}
	return nil
}

func (r *mctechStatementPreprocessor) rewriteStmt(mctx mctech.Context,
	stmt ast.Node, charset string, collation string) (dbs []string, skipped bool, err error) {
	dbs, skipped, err = isolation.ApplyExtension(mctx, stmt, charset, collation)

	failpoint.Inject("SetSQLDBS", func(v failpoint.Value) {
		str := v.(string)
		dbs = strings.Split(str, ",")
	})

	if skipped || err != nil {
		return dbs, skipped, err
	}

	// 判断sql中是否使用了是否包含'global_xxx'这样的数据库
	hasGlobalDb := slices.IndexFunc(dbs, func(db string) bool {
		return mctx.IsGlobalDb(db)
	}) >= 0

	if !hasGlobalDb {
		return dbs, false, nil
	}

	modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetSQLHasGlobalDB(true)
	result := mctx.PrepareResult()
	if result.Global() {
		// 启用global时，允许跨任意数据库查询
		return dbs, false, nil
	}

	// 未启用global,租户code为空，留到后续Validate步骤统一校验
	if result.Tenant() == "" && !mctx.UsingTenantParam() {
		return dbs, false, nil
	}

	modifyCtx.SetSQLRewrited(!skipped)
	return dbs, false, nil
}

// normalize 对db列表排序，去重
func normalize(dbs []string) []string {
	slices.Sort(dbs)

	newDbs := make([]string, 0, len(dbs))
	for _, db := range dbs {
		if slices.Contains(newDbs, db) {
			continue
		}
		newDbs = append(newDbs, db)
	}
	return newDbs
}

var preprocessor StatementPreprocessor = &mctechStatementPreprocessor{}
