package exprstatic

import (
	"errors"

	vsctx "github.com/pingcap/tidb/pkg/util/context"
)

// VSCtx returns ValueStoreContext of sessionctx.Context
func (ctx *EvalContext) VSCtx() vsctx.ValueStoreContext {
	// 仅为实现接口，填充接口函数
	panic(errors.ErrUnsupported)
}
