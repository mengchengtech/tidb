// add by zhangbing

package sessionexpr

import (
	vsctx "github.com/pingcap/tidb/pkg/util/context"
)

// VSCtx returns ValueStoreContext of sessionctx.Context
func (ctx *EvalContext) VSCtx() vsctx.ValueStoreContext {
	return ctx.sctx
}
