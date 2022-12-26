package preps

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/sessionctx"
)

const tenantOnlyRole = "tenant_only"

var tenantCodePattern = regexp.MustCompile(`(?i)^code_(.+)?$`)

func currentUser(ctx sessionctx.Context) string {
	vars := ctx.GetSessionVars()
	return vars.User.Username
}

func findTenantInfoFromRoles(ctx sessionctx.Context) (tenantOnly bool, tenantCode string, err error) {
	vars := ctx.GetSessionVars()
	tenantFromRoles := make([]string, 0, len(vars.ActiveRoles))
	for _, r := range vars.ActiveRoles {
		if r.Username == tenantOnlyRole {
			tenantOnly = true
			continue
		}
		subs := tenantCodePattern.FindStringSubmatch(r.Username)
		if subs != nil {
			tenantFromRoles = append(tenantFromRoles, subs[1])
		}
	}
	// var isAdmin = user == "root"

	tenantFromRolesLength := len(tenantFromRoles)
	// if !isAdmin && tenantFromRolesLength > 0 && tenantFromRolesLength != len(roleNames) {
	// 	// 1. 如果发现有一个code_{tenant} 角色，不能再有其他任何角色，否则报错
	// 	return tenantOnly, tenantCode, fmt.Errorf("当前用户%s同时属于多种类型的角色。", user)
	// }

	if tenantFromRolesLength > 0 {
		// 存在code_{tenant}角色，忽略global参数
		// 2. 如果是单一code_{tenant}角色，按自动补条件处理，tenant来自角色名
		tenantCode = tenantFromRoles[0]
		if tenantFromRolesLength > 1 {
			// 只有一个角色能提供租户信息
			for index := 1; index < tenantFromRolesLength; index++ {
				if tenantCode != tenantFromRoles[index] {
					user := currentUser(ctx)
					return tenantOnly, tenantCode, fmt.Errorf("用户%s所属的角色存在多个租户的信息", user)
				}
			}
		}
	}

	return tenantOnly, tenantCode, nil
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

func (p *sqlPreprocessor) Prepare(mctechCtx mctech.Context,
	actions []*actionInfo, params map[string]any) (*mctech.PrepareResult, error) {
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
			action, ok := resolveActions[act.name]
			if !ok {
				return nil, fmt.Errorf("不支持的action操作: %s", act.name)
			}

			sql, err := action.Resolve(p.preparedSQL, act.args, params)
			if err != nil {
				return nil, err
			}
			p.preparedSQL = sql
		}
	}

	tenantOnly, tenantCode, err := findTenantInfoFromRoles(mctechCtx.Session())
	if err != nil {
		return nil, err
	}

	result, err := mctech.NewPrepareResult(tenantCode, params)
	if err != nil {
		return nil, err
	}

	if result.Global() && tenantOnly {
		return nil, errors.New("当前数据库用户不允许启用 global hint")
	}

	return result, nil
}
