package preps

import (
	"errors"
	"fmt"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
)

type varValue struct {
	name  string
	value string
}

var mppOptionMap = map[string][]*varValue{
	"force": {
		{variable.TiDBIsolationReadEngines, "tidb,tiflash"},
		{variable.TiDBEnforceMPPExecution, variable.BoolToOnOff(true)},
	},
	"disable": {
		{variable.TiDBIsolationReadEngines, "tidb,tikv"},
	},
	// 新建连接默认值，不用设置
	"allow": {},
}

type tidbSessionMCTechContext struct {
	mctech.Context
	session    sessionctx.Context
	storedVars []varValue
}

// NewContext function
func NewContext(session sessionctx.Context, usingTenantParam bool) mctech.Context {
	baseCtx := mctech.NewBaseContext(usingTenantParam)
	return &tidbSessionMCTechContext{
		Context: baseCtx,
		session: session,
	}
}

func init() {
	mctech.NewContext = NewContext
}

func (d *tidbSessionMCTechContext) CurrentDB() string {
	return d.session.GetSessionVars().CurrentDB
}

func (d *tidbSessionMCTechContext) GetInfoForTest() string {
	info := d.Context.(mctech.ContextForTest).GetInfoForTest()
	return fmt.Sprintf("{%s,%s}", info, d.CurrentDB())
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
		var value string
		value, err = variable.GetSessionOrGlobalSystemVar(sessionVars, v.name)
		if err != nil {
			return
		}
		storedVars = append(storedVars, varValue{v.name, value})
	}
	d.storedVars = storedVars
	return
}

func (d *tidbSessionMCTechContext) ReloadSessionMPPVars() (err error) {
	sessionVars := d.Session().GetSessionVars()
	vars := d.storedVars
	// 把之前缓存的当前会话的原始值恢复到初始状态
	for _, v := range vars {
		err = variable.SetSessionSystemVar(sessionVars, v.name, v.value)
		if err != nil {
			return
		}
	}
	return
}

func (d *tidbSessionMCTechContext) SetSessionMPPVars(mpp string) (err error) {
	var targetVars []*varValue
	if targetVars, err = d.getTargetMPPVars(mpp); err != nil {
		return
	}

	// 修改mpp相关的会话级参数
	sessionVars := d.Session().GetSessionVars()
	for _, v := range targetVars {
		err = variable.SetSessionSystemVar(sessionVars, v.name, v.value)
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
