package prapared

import (
	"fmt"
	"regexp"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

var rolePattern = regexp.MustCompile(`(?i)^([^_]+)_internal_(read|write)$`)

func currentRoles(ctx sessionctx.Context) []string {
	vars := ctx.GetSessionVars()
	roles := make([]string, 0, len(vars.ActiveRoles))
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
	var isAdmin = user == "root"
	// if !isAdmin {
	// 	for _, role := range roleNames {
	// 		if role == "root" {
	// 			isAdmin = true
	// 			break
	// 		}
	// 	}
	// }

	tenantFromRolesLength := len(tenantFromRoles)
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

var valueFormatters = map[string]valueFormatter{
	mctech.ParamGlobal: newGlobalValueFormatter(),
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

func (p *sqlPreprocessor) Prepare(ctx sessionctx.Context,
	actions map[string]string, params map[string]any) (*mctech.PrapareResult, error) {
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

			sql, err := action.Resolve(p.preparedSQL, args, params)
			if err != nil {
				return nil, err
			}
			p.preparedSQL = sql
		}
	}

	tenant, err := findTenantCodeFromRole(ctx)
	if err != nil {
		return nil, err
	}
	return mctech.NewPrapareResult(tenant, params)
}