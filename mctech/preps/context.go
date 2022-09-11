package preps

import (
	"fmt"

	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/sessionctx"
)

type tidbSessionMCTechContext struct {
	mctech.Context
	session sessionctx.Context
}

// NewContext function
func NewContext(session sessionctx.Context) mctech.Context {
	return &tidbSessionMCTechContext{
		Context: mctech.NewBaseContext(),
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
