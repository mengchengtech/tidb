package preps

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/visitor"
	"github.com/pingcap/tidb/parser/ast"
	"golang.org/x/exp/slices"
)

var mctechHintPattern = regexp.MustCompile(`(?i)/\*&\s*(\$?[a-z_0-9]+)[:|]\s*(.*?)\s*\*/`)

// StatementPreprocessor interface
type StatementPreprocessor interface {
	ParseSQL(mctx mctech.Context, sql string) (string, mctech.ParseResult, error)
	ResolveStmt(mctx mctech.Context,
		stmt ast.Node, charset string, collation string) (schema mctech.StmtSchemaInfo, skipped bool, err error)
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
func (r *mctechStatementPreprocessor) ParseSQL(
	mctx mctech.Context, sql string) (string, mctech.ParseResult, error) {
	if mctx.ParseResult() != nil {
		return "", nil, errors.New("[mctech] ParseSQL failure, Context exists")
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
	var parsedSQL string
	comments := GetCustomCommentFromSQL(sql)
	result, err := preprocessor.Parse(mctx, actions, comments, params)
	if err != nil {
		return parsedSQL, nil, err
	}

	parsedSQL = preprocessor.sql
	return parsedSQL, result, nil
}

func (r *mctechStatementPreprocessor) Validate(mctx mctech.Context) error {
	result := mctx.ParseResult()
	// 执行到此处说明当前语句一定是DML或QUERY
	// sql没有被改写，但是用到了global_xxx数据库，并且没有设置global为true
	if !mctx.SQLRewrited() && mctx.SQLHasGlobalDB() &&
		!result.TenantOmit() {
		// 检查DML语句和QUERY语句改写状态
		return errors.New("当前用户无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */")
	}
	return nil
}

func (r *mctechStatementPreprocessor) ResolveStmt(mctx mctech.Context,
	stmt ast.Node, charset string, collation string) (schema mctech.StmtSchemaInfo, skipped bool, err error) {
	schema, skipped, err = visitor.ApplyExtension(mctx, stmt, charset, collation)

	if skipped || err != nil {
		return schema, skipped, err
	}

	// 判断sql中是否使用了是否包含'global_xxx'这样的数据库
	hasGlobalDb := slices.IndexFunc(schema.Databases, func(db string) bool {
		return mctx.IsGlobalDb(db)
	}) >= 0

	if !hasGlobalDb {
		return schema, false, nil
	}

	modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyCtx.SetSQLHasGlobalDB(true)
	result := mctx.ParseResult()
	if result.TenantOmit() {
		// 启用global时，允许跨任意数据库查询
		return schema, false, nil
	}

	// 未启用global,租户code为空，留到后续Validate步骤统一校验
	if result.Tenant().Code() == "" && !mctx.UsingTenantParam() {
		return schema, false, nil
	}

	modifyCtx.SetSQLRewrited(!skipped)
	return schema, false, nil
}

var preprocessor StatementPreprocessor = &mctechStatementPreprocessor{}
