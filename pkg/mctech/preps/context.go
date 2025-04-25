package preps

import (
	"errors"

	"github.com/pingcap/tidb/pkg/mctech"
	_ "github.com/pingcap/tidb/pkg/mctech/interceptor" // 为了解除循环依赖，触发运行期动态函数初始化，在此处强制加载
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type varValue struct {
	name  string
	value string
}

var mppOptionMap = map[string][]*varValue{
	"force": {
		{variable.TiDBIsolationReadEngines, "tidb,tiflash"},
		{variable.TiDBEnforceMPPExecution, variable.BoolToOnOff(true)},
		{variable.TiDBAllowMPPExecution, variable.BoolToOnOff(true)},
	},
	"disable": {
		{variable.TiDBIsolationReadEngines, "tidb,tikv"},
		{variable.TiDBEnforceMPPExecution, variable.BoolToOnOff(false)},
		{variable.TiDBAllowMPPExecution, variable.BoolToOnOff(false)},
	},
	// 新建连接默认值，不用设置
	"allow": {},
}

type tidbSessionMCTechContext struct {
	mctech.Context
	session    sessionctx.Context
	storedVars []varValue
}

// newContext function
func newContext(session sessionctx.Context, usingTenantParam bool) mctech.Context {
	baseCtx := mctech.NewBaseContext(usingTenantParam)
	baseCtx.(mctech.ModifyContext).SetDWSelector(newDWSelector())
	return &tidbSessionMCTechContext{
		Context: baseCtx,
		session: session,
	}
}

var (
	_ mctech.SessionMPPVarsContext = &tidbSessionMCTechContext{}
)

func init() {
	mctech.NewContext = newContext
}

func (d *tidbSessionMCTechContext) CurrentDB() string {
	return d.session.GetSessionVars().CurrentDB
}

func (d *tidbSessionMCTechContext) GetInfoForTest() map[string]any {
	info := d.Context.(mctech.ContextForTest).GetInfoForTest()
	db := d.CurrentDB()
	if len(db) > 0 {
		info["db"] = db
	}
	return info
}

func (d *tidbSessionMCTechContext) Session() sessionctx.Context {
	return d.session
}

func (d *tidbSessionMCTechContext) BaseContext() mctech.Context {
	return d.Context
}

// ------------------------------------------------

func (d *tidbSessionMCTechContext) StoreSessionMPPVars(mpp string) (err error) {
	// 根据传入参数转换成会话中添加的mpp相关的参数名和值
	var defaultVars []*varValue
	if defaultVars, err = d.getTargetMPPVars(mpp); err != nil {
		return
	}

	sessionVars := d.Session().GetSessionVars()
	storedVars := []varValue{}
	// 缓存当前会话中同名参数的值
	for _, v := range defaultVars {
		if value, ok := sessionVars.GetSystemVar(v.name); ok {
			storedVars = append(storedVars, varValue{v.name, value})
		}
	}
	d.storedVars = storedVars
	return
}

func (d *tidbSessionMCTechContext) ReloadSessionMPPVars() error {
	sessionVars := d.Session().GetSessionVars()
	vars := d.storedVars
	// 把之前缓存的当前会话的原始值恢复到初始状态
	for _, v := range vars {
		if err := sessionVars.SetSystemVar(v.name, v.value); err != nil {
			return err
		}
	}
	d.storedVars = nil
	return nil
}

func (d *tidbSessionMCTechContext) SetSessionMPPVars(mpp string) (err error) {
	var targetVars []*varValue
	if targetVars, err = d.getTargetMPPVars(mpp); err != nil {
		return
	}

	// 修改mpp相关的会话级参数
	sessionVars := d.Session().GetSessionVars()
	for _, v := range targetVars {
		err = sessionVars.SetSystemVar(v.name, v.value)
		if err != nil {
			return
		}
	}
	return
}

func (d *tidbSessionMCTechContext) getTargetMPPVars(mpp string) ([]*varValue, error) {
	if vars, ok := mppOptionMap[mpp]; ok {
		return vars, nil
	}

	return nil, errors.New("mpp值不正确。可选值为 force, allow, disable")
}

func (d *tidbSessionMCTechContext) Clear() {
	if err := d.ReloadSessionMPPVars(); err != nil {
		logutil.BgLogger().Warn("[ReloadSessionMPPVars] received error", zap.Error(err))
	}

	d.session.ClearValue(mctech.MCExecStmtVarKey)
	d.session.ClearValue(mctech.MCContextVarKey)
	d.session.ClearValue(mctech.MCRUDetailsCtxKey)
}

// FlagRoles custom roles
type mctechFlagRoles struct {
	tenantOmit bool // 是否跳过租户隔离，用于一些需要同步数据的特殊场景
	tenantOnly bool // 是否包含tenant_only 角色
	acrossDB   bool // 是否包含 across_db 角色。保留字段，暂时没有使用
}

func (r *mctechFlagRoles) TenantOmit() bool {
	return r.tenantOmit
}

func (r *mctechFlagRoles) TenantOnly() bool {
	return r.tenantOnly
}

func (r *mctechFlagRoles) AcrossDB() bool {
	return r.acrossDB
}

func newFlagRoles(tenantOnly, tenantOmit, acrossDB bool) (*mctechFlagRoles, error) {
	if tenantOnly && tenantOmit {
		return nil, errors.New("当前用户不允许同时包含'租户隔离'和'忽略租户'角色")
	}

	roles := &mctechFlagRoles{
		tenantOnly: tenantOnly,
		tenantOmit: tenantOmit,
		acrossDB:   acrossDB,
	}
	return roles, nil
}

// NewFlagRoles create FlagRoles instance
func NewFlagRoles(tenantOnly, tenantOmit, acrossDB bool) (mctech.FlagRoles, error) {
	return newFlagRoles(tenantOnly, tenantOmit, acrossDB)
}
