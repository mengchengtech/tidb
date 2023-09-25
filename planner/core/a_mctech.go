// add by zhangbing

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"golang.org/x/exp/slices"
)

func (b *PlanBuilder) buildMCTech(ctx context.Context, stmt *ast.MCTechStmt) (Plan, error) {
	p := &MCTech{
		Format:   stmt.Format,
		ExecStmt: stmt.Stmt,
	}
	p.ctx = b.ctx
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
	switch {
	case format == types.ExplainFormatROW:
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
func (e *MCTech) mctechPlanInRowFormat(ctx context.Context) error {
	mctx, err := mctech.GetContext(ctx)
	if err != nil {
		return err
	}

	// "global", "excludes", "tenant", "tenant_from_role", "dw_index", "params", "prepared_sql"
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreBracketAroundBinaryOperation, &sb)
	err = e.ExecStmt.Restore(restoreCtx)
	if err != nil {
		return err
	}

	var (
		global     = false
		excludes   = []string{}
		tenant     string
		tenantFrom = "none"
		params     = map[string]any{}
		db         = e.ctx.GetSessionVars().CurrentDB
		index      = mctech.DbIndex(-1)
		restoreSQL = sb.String()
	)

	if mctx != nil {
		result := mctx.PrepareResult()
		if result != nil {
			global = result.Global()
			params = result.Params()
			excludes = result.Excludes()
			tenant = result.Tenant()
			if tenant != "" {
				if result.TenantFromRole() {
					tenantFrom = "role"
				} else {
					tenantFrom = "hint"
				}
			}
		}
		index, err = mctx.GetDbIndex()
		if err != nil {
			err = nil
			index = -1
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

func (e *Execute) getExtensionParams(ctx context.Context) ([]ast.ParamMarkerExpr, int) {
	prepared := e.PrepStmt.PreparedAst
	index := slices.IndexFunc(prepared.Params, func(p ast.ParamMarkerExpr) bool {
		return p.GetOffset() == mctech.ExtensionParamMarkerOffset
	})

	if index < 0 {
		return nil, -1
	}
	return prepared.Params[index:], index
}

type extensionArgCreator[T any] func() (T, error)

func appendExtensionArgs[T any](
	params []ast.ParamMarkerExpr, callback extensionArgCreator[T]) ([]T, error) {
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

// AppendVarExprs append custom extension parameters
func (e *Execute) AppendVarExprs(ctx context.Context) error {
	// 获取扩展参数列表
	extParams, from := e.getExtensionParams(ctx)
	if len(extParams) == 0 {
		return nil
	}

	mctx, err := mctech.GetContext(ctx)
	if err != nil {
		return err
	}

	var tenantValue expression.Expression
	if mctx != nil {
		// 优先从sql语句中提取租户信息
		if result := mctx.PrepareResult(); result != nil {
			tenantCode := result.Tenant()
			tenantValue = expression.DatumToConstant(types.NewDatum(tenantCode), mysql.TypeString, 0)
		}
	}

	if tenantValue == nil {
		// 其次从参数中提取租户变量
		if len(e.Params) > from {
			tenantValue = e.Params[from]
			// 暂时先移除参数租户参数
			e.Params = e.Params[:from]
		}
	}

	sctx := mctx.Session()
	sessionVars := sctx.GetSessionVars()
	if tenantValue == nil {
		user := sessionVars.User.Username
		return fmt.Errorf("当前用户%s无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */", user)
	}

	extArgs, err := appendExtensionArgs(extParams, func() (expression.Expression, error) {
		return tenantValue, nil
	})

	if err == nil {
		e.Params = append(e.Params, extArgs...)
	}
	return err
}
