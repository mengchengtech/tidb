// add by zhangbing

package ast

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
)

var (
	_ StmtNode = &MCTechStmt{}
)

const (
	// mctech function.

	// 别名
	MCSeq             = "mc_seq"
	MCVersionJustPass = "mc_version_just_pass"
	MCDecrypt         = "mc_decrypt"
	MCEncrypt         = "mc_encrypt"
	MCSeqDecode       = "mc_seq_decode"
	MCGetFullSQL      = "mc_get_full_sql"
	MCHelp            = "mc_help"
	MCDWIndexInfo     = "mc_dw_index_info"

	// 全名
	MCTechSequence        = "mctech_sequence"
	MCTechVersionJustPass = "mctech_version_just_pass"
	MCTechDecrypt         = "mctech_decrypt"
	MCTechEncrypt         = "mctech_encrypt"
	MCTechSequenceDecode  = "mctech_sequence_decode"
	MCTechGetFullSQL      = "mctech_get_full_sql"
	MCTechHelp            = "mctech_help"
	// 获取数仓库的索引信息
	MCTechDataWarehouseIndexInfo = "mctech_get_data_warehouse_index_info"
)

type MCTechStmtOp string

const (
	MCTechStmtOpDesc                    = "Desc"
	MCTechStmtOpSeqDecode               = "SeqDecode"
	MCTechStmtOpShowDWIndex             = "ShowDWIndex"
	MCTechStmtOpShowHelp                = "ShowHelp"
	MCTechStmtOpShowDenyDigest          = "ShowDenyDigest"
	MCTechStmtOpShowDatabaseConstraints = "ShowDatabaseConstraints"
	MCTechStmtOpShowFullSQL             = "ShowFullSql"
)

type SeqDecodeOption struct {
	SeqValue int64
}

type ShowHelpOption struct {
	ShowHidden bool
}

type ShowFullSQLOption struct {
	RunConnID uint64
	RunTxID   uint64
	RunAt     string
	Group     string
}

type ShowDescOption struct {
	Stmt   StmtNode
	Format string
}

type MCTechStmt struct {
	stmtNode
	Type        MCTechStmtOp
	SeqDecode   *SeqDecodeOption
	ShowHelp    *ShowHelpOption
	ShowFullSQL *ShowFullSQLOption
	ShowDesc    *ShowDescOption
}

// Restore implements Node interface.
func (n *MCTechStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("MCTECH ")
	switch n.Type {
	case MCTechStmtOpDesc:
		if strings.ToLower(n.ShowDesc.Format) != "row" {
			ctx.WriteKeyWord("FORMAT")
			ctx.WritePlain(" = ")
			ctx.WriteString(n.ShowDesc.Format)
			ctx.WritePlain(" ")
		}
		if err := n.ShowDesc.Stmt.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore MCTechStmt.Stmt")
		}
	case MCTechStmtOpSeqDecode:
		ctx.WriteKeyWord("SEQ_DECODE")
		ctx.WritePlain(" ")
		ctx.WritePlain(strconv.FormatInt(n.SeqDecode.SeqValue, 10))
	case MCTechStmtOpShowDWIndex, MCTechStmtOpShowHelp,
		MCTechStmtOpShowDatabaseConstraints, MCTechStmtOpShowFullSQL, MCTechStmtOpShowDenyDigest:
		ctx.WriteKeyWord("SHOW")
		ctx.WritePlain(" ")
		switch n.Type {
		case MCTechStmtOpShowDenyDigest:
			ctx.WriteKeyWord("DENY_DIGEST")
		case MCTechStmtOpShowDWIndex:
			ctx.WriteKeyWord("DW_INDEX")
		case MCTechStmtOpShowHelp:
			ctx.WriteKeyWord("HELP")
			if n.ShowHelp.ShowHidden {
				ctx.WritePlain(" ")
				ctx.WriteKeyWord("TRUE")
			}
		case MCTechStmtOpShowDatabaseConstraints:
			ctx.WriteKeyWord("DATABASE")
			ctx.WritePlain(" ")
			ctx.WriteKeyWord("CONSTRAINTS")
		case MCTechStmtOpShowFullSQL:
			ctx.WriteKeyWord("FULL_SQL")
			ctx.WritePlain(" ")
			ctx.WriteString(n.ShowFullSQL.RunAt)
			ctx.WritePlain(" ")
			ctx.WritePlain(strconv.FormatUint(n.ShowFullSQL.RunConnID, 10))
			ctx.WritePlain(" ")
			ctx.WritePlain(strconv.FormatUint(n.ShowFullSQL.RunTxID, 10))
			if n.ShowFullSQL.Group != "" {
				ctx.WritePlain(" ")
				ctx.WriteString(n.ShowFullSQL.Group)
			}
		}
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
	if n.ShowDesc != nil {
		node, ok := n.ShowDesc.Stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ShowDesc.Stmt = node.(StmtNode)
	}
	return v.Leave(n)
}
