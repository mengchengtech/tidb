package preps

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

const tenantOnlyRole = "tenant_only"
const acrossDBRole = "across_db"

var tenantCodePattern = regexp.MustCompile(`(?i)^code_(.+)?$`)

func currentUser(ctx sessionctx.Context) string {
	vars := ctx.GetSessionVars()
	return vars.User.Username
}

func resolveActiveRoles(ctx sessionctx.Context) (roles mctech.FlagRoles, tenantCode string, err error) {
	vars := ctx.GetSessionVars()
	var (
		tenantOnly bool // 角色上是否存在只允许限制在某个租户上执行的角色
		acrossDB   bool // 角色上是否存在支持跨库查询的标记角色。
	)
	tenantFromRoles := make([]string, 0, len(vars.ActiveRoles))
	for _, r := range vars.ActiveRoles {
		switch r.Username {
		case tenantOnlyRole:
			tenantOnly = true
		case acrossDBRole:
			acrossDB = true
		default:
			subs := tenantCodePattern.FindStringSubmatch(r.Username)
			if subs != nil {
				tenantFromRoles = append(tenantFromRoles, subs[1])
			}
		}
	}
	// var isAdmin = user == "root"

	tenantFromRolesLength := len(tenantFromRoles)
	// if !isAdmin && tenantFromRolesLength > 0 && tenantFromRolesLength != len(roleNames) {
	// 	// 1. 如果发现有一个code_{tenant} 角色，不能再有其他任何角色，否则报错
	// 	return tenantOnly, tenantCode, fmt.Errorf("当前用户%s同时属于多种类型的角色。", user)
	// }

	if tenantFromRolesLength > 0 {
		// 角色上有租户信息，也当作仅允许租户账号访问模式
		tenantOnly = true

		// 存在code_{tenant}角色，忽略global参数
		// 2. 如果是单一code_{tenant}角色，按自动补条件处理，tenant来自角色名
		tenantCode = tenantFromRoles[0]
		if tenantFromRolesLength > 1 {
			// 只有一个角色能提供租户信息
			for index := 1; index < tenantFromRolesLength; index++ {
				if tenantCode != tenantFromRoles[index] {
					user := currentUser(ctx)
					return nil, "", fmt.Errorf("用户%s所属的角色存在多个租户的信息", user)
				}
			}
		}
	}

	return NewFlagRoles(tenantOnly, acrossDB), tenantCode, nil
}

var valueFormatters = map[string]valueFormatter{
	mctech.ParamGlobal:      newGlobalValueFormatter(),
	mctech.ParamAcross:      newCrossValueFormatter(),
	mctech.ParamImpersonate: newEnumValueFormatter("tenant_only"),
}

var resolveActions = map[string]action{
	"replace": &replaceAction{},
}

type sqlPreprocessor struct {
	preparedSQL string
}

func newSQLPreprocessor(stmt string) *sqlPreprocessor {
	return &sqlPreprocessor{
		preparedSQL: stmt,
	}
}

func (p *sqlPreprocessor) Prepare(mctx mctech.Context, actions []ActionInfo,
	comments mctech.Comments, params map[string]any) (mctech.PrepareResult, error) {
	if len(params) > 0 {
		for name, formatter := range valueFormatters {
			value := params[name]
			text, ok := value.(string)
			if !ok {
				continue
			}

			formatted, err := formatter.Format(name, text)
			if err != nil {
				return nil, err
			}

			params[name] = formatted
		}
	}

	if len(actions) > 0 {
		for _, act := range actions {
			action, ok := resolveActions[act.Name()]
			if !ok {
				return nil, fmt.Errorf("不支持的action操作: %s", act.Name())
			}

			sql, err := action.Resolve(p.preparedSQL, act.Args(), params)
			if err != nil {
				return nil, err
			}
			p.preparedSQL = sql
		}
	}

	roles, tenantCode, err := resolveActiveRoles(mctx.Session())
	if err != nil {
		return nil, err
	}

	if v, ok := params[mctech.ParamImpersonate]; ok {
		if role := v.(string); role == tenantOnlyRole {
			roles.SetTenantOnly(true)
		}
	}

	result, err := mctech.NewPrepareResult(tenantCode, roles, comments, params)
	if err != nil {
		return nil, err
	}

	if result.Global() && roles.TenantOnly() {
		return nil, errors.New("当前数据库用户包含租户隔离角色，不允许启用 global hint")
	}

	return result, nil
}
