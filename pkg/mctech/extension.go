package mctech

import (
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// SessionStmtEventInfo 自定义事件参数
type SessionStmtEventInfo interface {
	extension.StmtEventInfo
	// SCtx
	SCtx() sessionctx.Context
}

func onStmtEvent(tp extension.StmtEventTp, event extension.StmtEventInfo) {
	var (
		info SessionStmtEventInfo
		ok   bool
	)

	if info, ok = event.(SessionStmtEventInfo); !ok {
		return
	}
	it := GetInterceptor()
	switch tp {
	case extension.StmtError:
		err := info.GetError()
		it.AfterHandleStmt(info.SCtx(), info.StmtNode(), err)
	case extension.StmtSuccess:
		it.AfterHandleStmt(info.SCtx(), info.StmtNode(), nil)
	}
}

// RegisterExtensions 注册自定义事件扩展
func RegisterExtensions() error {
	option := extension.WithSessionHandlerFactory(func() *extension.SessionHandler {
		return &extension.SessionHandler{
			OnStmtEvent: onStmtEvent,
		}
	})
	return extension.Register("mctech", option)
}
