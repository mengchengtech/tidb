package preps

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/sessionctx"
)

const tenantOmitRole = "tenant_omit"
const tenantOnlyRole = "tenant_only"
const acrossDBRole = "across_db"

var tenantCodePattern = regexp.MustCompile(`(?i)^code_(.+)?$`)

func fetchActiveRoles(ctx sessionctx.Context, forceTenantOnly bool) (roles *mctechFlagRoles, tenantCode string, err error) {
	vars := ctx.GetSessionVars()
	var (
		tenantOnly bool // 角色上是否存在只允许限制在某个租户上执行的角色
		acrossDB   bool // 角色上是否存在支持跨库查询的标记角色。
		tenantOmit bool // 角色上是否存在忽略租户隔离的标记角色
	)
	tenantFromRoles := make([]string, 0, len(vars.ActiveRoles))
	for _, r := range vars.ActiveRoles {
		switch r.Username {
		case tenantOnlyRole:
			tenantOnly = true
		case acrossDBRole:
			acrossDB = true
		case tenantOmitRole:
			tenantOmit = true
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
	// 	return tenantOnly, tenantCode, errors.New("当前用户同时属于多种类型的角色。")
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
					return nil, "", errors.New("当前用户所属的角色存在多个租户的信息")
				}
			}
		}
	}

	if forceTenantOnly {
		tenantOnly = true
	}

	if roles, err = newFlagRoles(tenantOnly, tenantOmit, acrossDB); err != nil {
		return nil, "", err
	}

	return roles, tenantCode, err
}

var valueFormatters = map[string]valueFormatter{
	mctech.ParamGlobal:      newGlobalValueFormatter(),
	mctech.ParamAcross:      newCrossValueFormatter(),
	mctech.ParamImpersonate: newEnumValueFormatter(tenantOnlyRole),
}

var resolveActions = map[string]action{
	"replace": &replaceAction{},
}

type sqlPreprocessor struct {
	sql string
}

func newSQLPreprocessor(sql string) *sqlPreprocessor {
	return &sqlPreprocessor{sql: sql}
}

func (p *sqlPreprocessor) Parse(mctx mctech.Context, actions []ActionInfo,
	comments mctech.Comments, params map[string]any) (mctech.ParseResult, error) {
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

			sql, err := action.Resolve(p.sql, act.Args(), params)
			if err != nil {
				return nil, err
			}
			p.sql = sql
		}
	}

	var forceTenantOnly = false
	if v, ok := params[mctech.ParamImpersonate]; ok {
		if role := v.(string); role == tenantOnlyRole {
			forceTenantOnly = true
		}
	}

	roles, tenantCode, err := fetchActiveRoles(mctx.Session(), forceTenantOnly)
	if err != nil {
		return nil, err
	}

	result, err := mctech.NewParseResult(tenantCode, roles, comments, params)
	if err != nil {
		return nil, err
	}

	if result.TenantOmit() && roles.TenantOnly() {
		return nil, errors.New("当前用户包含'租户隔离'角色，不允许启用 'global' hint")
	}

	return result, nil
}
