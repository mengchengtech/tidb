package sessionctx

import (
	"strings"

	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap/zapcore"
)

// MCTechExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type MCTechExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k MCTechExecStmtVarKeyType) String() string {
	return "mctech___exec_stmt_var_key"
}

// MCTechExecStmtVarKey is a variable key for ExecStmt.
const MCTechExecStmtVarKey MCTechExecStmtVarKeyType = 0

// ShortInfo resolve information from current session.
func ShortInfo(sctx Context) ShortSessionInfo {
	var si = &shortSessionInfo{}
	sessVars := sctx.GetSessionVars()
	si.db = strings.ToLower(sessVars.CurrentDB)
	si.host = variable.GetSysVar(variable.Hostname).Value

	if sessVars.User != nil {
		si.user = sessVars.User.Username
		si.client = sessVars.User.Hostname
	}

	return si
}

// ShortSessionInfo current session user info
type ShortSessionInfo interface {
	zapcore.ObjectMarshaler
	GetUser() string
	GetDB() string
	GetClient() string
	GetHost() string
}

type shortSessionInfo struct {
	user   string
	db     string
	client string
	host   string
}

func (si *shortSessionInfo) GetUser() string {
	return si.user
}

func (si *shortSessionInfo) GetDB() string {
	return si.db
}

func (si *shortSessionInfo) GetClient() string {
	return si.client
}

func (si *shortSessionInfo) GetHost() string {
	return si.host
}

const mctechLogFilterToken = "!@#$mctech$#@!"

func (si *shortSessionInfo) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("token", mctechLogFilterToken)
	enc.AddString("user", si.user)
	enc.AddString("db", si.db)
	enc.AddString("client", si.client)
	enc.AddString("host", si.host)
	return nil
}
