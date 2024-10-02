package mctech

import (
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/terror"
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
		if info.StmtNode() != nil {
			// stmtNode存在，sql解析成功，执行失败
			it.AfterHandleStmt(info.SCtx(), info.StmtNode(), err)
		} else {
			// stmtNode 不存在，sql 解析失败
			it.ParseSQLFailed(info.SCtx(), info.OriginalText(), err)
		}
	case extension.StmtSuccess:
		it.AfterHandleStmt(info.SCtx(), info.StmtNode(), nil)
	}
}

// RegisterExtensions 注册自定义事件扩展
func RegisterExtensions() {
	option := extension.WithSessionHandlerFactory(func() *extension.SessionHandler {
		return &extension.SessionHandler{
			OnStmtEvent: onStmtEvent,
		}
	})
	err := extension.Register("mctech", option)
	terror.MustNil(err)
}
