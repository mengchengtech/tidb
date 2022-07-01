package prapared

import (
	"fmt"
	"regexp"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/sessionctx"
)

const SUPER_ADMIN_ROLE = "root"

var rolePattern = regexp.MustCompile(`(?i)^([^_]+)_internal_(read|write)$`)

func currentRoles(ctx sessionctx.Context) []string {
	vars := ctx.GetSessionVars()
	roles := make([]string, len(vars.ActiveRoles))
	for _, r := range vars.ActiveRoles {
		roles = append(roles, r.Username)
	}
	return roles
}

func currentUser(ctx sessionctx.Context) string {
	vars := ctx.GetSessionVars()
	return vars.User.Username
}

func findTenantCodeFromRole(ctx sessionctx.Context) (string, error) {
	roleNames := currentRoles(ctx)
	tenantFromRoles := []string{}
	for _, role := range roleNames {
		subs := rolePattern.FindStringSubmatch(role)
		if subs != nil {
			tenantFromRoles = append(tenantFromRoles, subs[1])
		}
	}

	user := currentUser(ctx)
	tenantFromRolesLength := len(tenantFromRoles)
	var isAdmin = false
	for _, role := range roleNames {
		if role == SUPER_ADMIN_ROLE {
			isAdmin = true
			break
		}
	}

	if !isAdmin && tenantFromRolesLength > 0 && tenantFromRolesLength != len(roleNames) {
		// 1. 如果发现有一个{tenant}_internal_r/w角色，不能再有其他任何角色，否则报错
		return "", fmt.Errorf("当前用户%s同时属于多种类型的角色。", user)
	}

	if tenantFromRolesLength > 0 {
		// 存在{tenant}_internal_r/w角色，忽略global参数
		// 2. 如果是单一{tenant}_internal_r/w角色，按自动补条件处理，tenant来自角色名
		tenant := tenantFromRoles[0]
		if tenantFromRolesLength == 1 {
			// 只有一个角色能提供租户信息
			return tenant, nil
		}

		for index := 1; index < tenantFromRolesLength; index++ {
			if tenant != tenantFromRoles[index] {
				return "", fmt.Errorf("用户%s所属的角色存在多个租户的信息", user)
			}
		}

		// 所有角色提取的租户信息都相同
		return tenant, nil
	}

	return "", nil
}

var valueFormatters = map[string]ValueFormatter{
	mctech.PARAM_GLOBAL: NewGlobalValueFormatter(),
}

var resolveActions = map[string]Action{
	"replace": &ReplaceAction{},
}

type SqlPreprocessor struct {
	preparedSql string
}

func NewSqlPreprocessor(stmt string) *SqlPreprocessor {
	return &SqlPreprocessor{
		preparedSql: stmt,
	}
}

func (p *SqlPreprocessor) Prepare(ctx sessionctx.Context,
	actions map[string]string, params map[string]any) (*mctech.ResolveResult, error) {
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
		for actionName, args := range actions {
			action, ok := resolveActions[actionName]
			if !ok {
				return nil, fmt.Errorf("不支持的action操作: %s", actionName)
			}

			sql, err := action.Resolve(p.preparedSql, args, params)
			if err != nil {
				return nil, err
			}
			p.preparedSql = sql
		}
	}

	tenant, err := findTenantCodeFromRole(ctx)
	if err != nil {
		return nil, err
	}
	return mctech.NewResolveResult(tenant, params)
}
