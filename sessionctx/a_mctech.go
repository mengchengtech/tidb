package sessionctx

import "strings"

// MCTechExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type MCTechExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k MCTechExecStmtVarKeyType) String() string {
	return "mctech___exec_stmt_var_key"
}

// MCTechExecStmtVarKey is a variable key for ExecStmt.
const MCTechExecStmtVarKey MCTechExecStmtVarKeyType = 0

// ResolveSession resolve information from current session.
func ResolveSession(sctx Context) (db string, user string, client string) {
	sessVars := sctx.GetSessionVars()
	db = strings.ToLower(sessVars.CurrentDB)
	if sessVars.User != nil {
		user = sessVars.User.Username
		client = sessVars.User.Hostname
		if sessVars.ConnectionInfo != nil {
			client = sessVars.ConnectionInfo.ClientIP
		}
	}

	return db, user, client
}
