package preps

import (
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

type tidbSessionMCTechContext struct {
	mctech.Context
	session sessionctx.Context
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
