package mctech

import "math"

// TenantParamMarkerOffset 添加的租户条件假的文本位置偏移量
const TenantParamMarkerOffset = math.MaxInt - 1

// MCExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type MCExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k MCExecStmtVarKeyType) String() string {
	return "mc___exec_stmt_var_key"
}

// MCExecStmtVarKey is a variable key for ExecStmt.
const MCExecStmtVarKey MCExecStmtVarKeyType = 0

// -----------------------------------------------------------------

// MCContextVarKeyType is a dummy type to avoid naming collision in context.
type MCContextVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k MCContextVarKeyType) String() string {
	return "mc___context_var_key"
}

// MCContextVarKey is a variable key for ExecStmt.
const MCContextVarKey MCContextVarKeyType = 0

// -----------------------------------------------------------------

// MCRUDetailsCtxKeyType is a dummy type to avoid naming collision in context.
type MCRUDetailsCtxKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k MCRUDetailsCtxKeyType) String() string {
	return "mc___ru_details_var_key"
}

// MCRUDetailsCtxKey is a variable key for ExecStmt.
const MCRUDetailsCtxKey MCRUDetailsCtxKeyType = 0
