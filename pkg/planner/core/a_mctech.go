// add by zhangbing

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

func (b *PlanBuilder) buildMCTech(ctx context.Context, stmt *ast.MCTechStmt) (Plan, error) {
	p := &MCTech{
		Format:   stmt.Format,
		ExecStmt: stmt.Stmt,
	}
	p.SetSCtx(b.ctx)
	return p, p.prepareSchema()
}

// MCTech represents a mctech plan.
type MCTech struct {
	baseSchemaProducer

	Format   string
	ExecStmt ast.StmtNode
	Rows     [][]*types.Datum
}

type columnDef struct {
	name    string
	colType byte
	size    int
}

var (
	longSize, _     = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLong)
	longlongSize, _ = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
)

var columnDefs = []*columnDef{
	{"global", mysql.TypeLonglong, longlongSize},
	{"excludes", mysql.TypeVarchar, 128},
	{"tenant", mysql.TypeVarchar, 50},
	{"tenant_from", mysql.TypeVarchar, 10},
	{"db", mysql.TypeVarchar, 50},
	{"dw_index", mysql.TypeLong, longSize},
	{"params", mysql.TypeVarchar, 512},
	{"prepared_sql", mysql.TypeString, mysql.MaxBlobWidth},
}

// prepareSchema prepares mctech's result schema.
func (e *MCTech) prepareSchema() error {
	format := strings.ToLower(e.Format)
	if format == types.ExplainFormatTraditional {
		format = types.ExplainFormatROW
		e.Format = types.ExplainFormatROW
	}
	switch format {
	case types.ExplainFormatROW:
		length := len(columnDefs)
		cwn := &columnsWithNames{
			cols:  make([]*expression.Column, 0, length),
			names: make([]*types.FieldName, 0, length),
		}

		for _, def := range columnDefs {
			column, field := buildColumnWithName("", def.name, def.colType, def.size)
			cwn.Append(column, field)
		}
		e.SetSchema(cwn.col2Schema())
		e.names = cwn.names
	default:
		return errors.Errorf("mctech format '%s' is not supported now", e.Format)
	}
	return nil
}

// RenderResult renders the mctech result as specified format.
func (e *MCTech) RenderResult(ctx context.Context) error {
	switch strings.ToLower(e.Format) {
	case types.ExplainFormatROW:
		if err := e.mctechPlanInRowFormat(ctx); err != nil {
			return err
		}
	default:
		return errors.Errorf("mctech format '%s' is not supported now", e.Format)
	}
	return nil
}

// explainPlanInRowFormat generates mctech information for root-tasks.
func (e *MCTech) mctechPlanInRowFormat(ctx context.Context) (err error) {
	mctechCtx := mctech.GetContext(ctx)
	// "global", "excludes", "tenant", "tenant_from_role", "dw_index", "params", "prepared_sql"
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreBracketAroundBinaryOperation, &sb)
	err = e.ExecStmt.Restore(restoreCtx)
	if err != nil {
		return err
	}

	var (
		global     bool
		excludes   = []string{}
		tenant     string
		tenantFrom = "none"
		params     = map[string]any{}
		db         = e.SCtx().GetSessionVars().CurrentDB
		index      = mctech.DbIndex(-1)
		restoreSQL = sb.String()
	)

	if mctechCtx != nil {
		pr := mctechCtx.PrepareResult()
		global = pr.Global()
		params = pr.Params()
		excludes = pr.Excludes()
		tenant = pr.Tenant()
		if tenant != "" {
			if pr.TenantFromRole() {
				tenantFrom = "role"
			} else {
				tenantFrom = "hint"
			}
		}
		index, err = mctechCtx.GetDbIndex()
		if err != nil {
			return err
		}
	}

	js, err := json.Marshal(params)
	if err != nil {
		return err
	}

	var row = []*types.Datum{
		createDatum(global),
		createDatum(strings.Join(excludes, ",")),
		createDatum(tenant),
		createDatum(tenantFrom),
		createDatum(db),
		createDatum(int(index)),
		createDatum(js),
		createDatum(restoreSQL),
	}
	e.Rows = append(e.Rows, row)
	return nil
}

func createDatum(value any) *types.Datum {
	d := &types.Datum{}
	d.SetValueWithDefaultCollation(value)
	return d
}

// isDefaultValMCSymFunc checks whether default value is a MCTECH_SEQUENCE() builtin function.
func isDefaultValMCSymFunc(expr ast.ExprNode) bool {
	if funcCall, ok := expr.(*ast.FuncCallExpr); ok {
		if funcCall.FnName.L == ast.MCTechSequence {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------

const tenantParamName = "__mc_tenant_code"

var tenantParamTp *types.FieldType

func init() {
	tenantParamTp = &types.FieldType{}
	tenantParamTp.SetType(mysql.TypeVarString)
	tenantParamTp.SetFlen(types.UnspecifiedLength)
	tenantParamTp.SetDecimal(types.UnspecifiedLength)
}

func (e *Execute) GetExtensionParams(ctx context.Context) ([]types.ParamMarkerOffset, string, error) {
	var (
		extParams  []types.ParamMarkerOffset
		tenantCode string
		err        error
	)
	prepared := e.PrepStmt.PreparedAst
	mctechCtx := mctech.GetContext(ctx)
	index := slices.IndexFunc(prepared.Params, func(p ast.ParamMarkerExpr) bool {
		return p.(types.ParamMarkerOffset).GetOffset() == mctech.ExtensionParamMarkerOffset
	})
	sessionVars := mctechCtx.Session().GetSessionVars()
	if index >= 0 {
		// 含有扩展参数
		tenantCode = mctechCtx.PrepareResult().Tenant()
		if tenantCode != "" {
			for _, v := range prepared.Params[index:] {
				extParams = append(extParams, v.(types.ParamMarkerOffset))
			}
		} else {
			user := sessionVars.User.Username
			err = fmt.Errorf("当前用户%s无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */", user)
		}
	}
	return extParams, tenantCode, err
}

func (e *Execute) InitSessionVars(ctx sessionctx.Context, execCom bool, tenantCode string) {
	sessionVars := ctx.GetSessionVars()
	if execCom {
		// execute command
		sessionVars.UnsetUserVar(tenantParamName)
	} else {
		// execute sql
		sessionVars.SetStringUserVar(tenantParamName, tenantCode, "")
	}
}

type extensionArgCreator[T any] func() (T, error)

func appendExtensionArgs[T any](
	params []types.ParamMarkerOffset, callback extensionArgCreator[T]) ([]T, error) {
	extensions := []T{}
	for _, p := range params {
		if p.GetOffset() == mctech.ExtensionParamMarkerOffset {
			// 扩展自定义参数
			if item, err := callback(); err != nil {
				return nil, err
			} else {
				extensions = append(extensions, item)
			}
		}
	}

	return extensions, nil
}

func (e *Execute) AppendVarExprs(sctx sessionctx.Context, extParams []types.ParamMarkerOffset, tenantCode string) error {
	extArgs, err := appendExtensionArgs(extParams, func() (expression.Expression, error) {
		return expression.BuildGetVarFunction(sctx,
			expression.DatumToConstant(types.NewDatum(tenantParamName), mysql.TypeString, 0),
			tenantParamTp)
	})
	if err == nil {
		e.Params = append(e.Params, extArgs...)
	}
	return err
}
