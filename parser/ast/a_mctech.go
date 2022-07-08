package ast

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/format"
)

var (
	_ StmtNode = &MCTechStmt{}
)

const (
	// mctech function.
	MCTechSequence        = "mctech_sequence"
	MCTechVersionJustPass = "mctech_version_just_pass"
	MCTechDecrypt         = "mctech_decrypt"
	MCTechEncrypt         = "mctech_encrypt"
)

type MCTechStmt struct {
	stmtNode

	Stmt   StmtNode
	Format string
}

// Restore implements Node interface.
func (n *MCTechStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("MCTECH ")
	if strings.ToLower(n.Format) != "row" {
		ctx.WriteKeyWord("FORMAT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if err := n.Stmt.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while restore MCTechStmt.Stmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *MCTechStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*MCTechStmt)
	node, ok := n.Stmt.Accept(v)
	if !ok {
		return n, false
	}
	n.Stmt = node.(StmtNode)
	return v.Leave(n)
}
