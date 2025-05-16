// add by zhangbing

package contextimpl

import (
	vsctx "github.com/pingcap/tidb/pkg/util/context"
)

// VSCtx returns ValueStoreContext of sessionctx.Context
func (ctx *SessionEvalContext) VSCtx() vsctx.ValueStoreContext {
	return ctx.sctx
}
