// add by zhangbing

package core

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

func (b *PlanBuilder) buildMCTech(_ context.Context, stmt *ast.MCTechStmt) (Plan, error) {
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

func getDefaultFieldLength(tp byte) int {
	flen, _ := mysql.GetDefaultFieldLengthAndDecimal(tp)
	return flen
}

var columnDefs = []*columnDef{
	{"global", mysql.TypeTiny, 1},
	{"excludes", mysql.TypeVarchar, 1024},
	{"includes", mysql.TypeVarchar, 1024},
	{"comments", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
	{"tenant", mysql.TypeVarchar, 50},
	{"tenant_from", mysql.TypeVarchar, 10},
	{"db", mysql.TypeVarchar, 50},
	{"dw_index", mysql.TypeTiny, getDefaultFieldLength(mysql.TypeTiny)},
	{"params", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
	{"prepared_sql", mysql.TypeString, getDefaultFieldLength(mysql.TypeString)},
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
func (e *MCTech) RenderResult(_ context.Context) error {
	switch strings.ToLower(e.Format) {
	case types.ExplainFormatROW:
		if err := e.mctechPlanInRowFormat(); err != nil {
			return err
		}
	default:
		return errors.Errorf("mctech format '%s' is not supported now", e.Format)
	}
	return nil
}

// explainPlanInRowFormat generates mctech information for root-tasks.
func (e *MCTech) mctechPlanInRowFormat() (err error) {
	sctx, err := AsSctx(e.SCtx())
	if err != nil {
		return err
	}

	var mctx mctech.Context
	if mctx, err = mctech.GetContext(sctx); err != nil {
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
		global     *bool
		excludes   []string
		includes   []string
		comments   = map[string]any{}
		tenant     *string
		tenantFrom *string
		params     = map[string]any{}
		db         = e.SCtx().GetSessionVars().CurrentDB
		index      *int
		restoreSQL = sb.String()
	)

	if mctx != nil {
		result := mctx.PrepareResult()
		if result != nil {
			global = toPtr(result.TenantOmit())
			params = result.Params()
			excludes = result.Global().Excludes()
			includes = result.Global().Includes()
			comments = result.Comments().ToMap()
			tenantCode := result.Tenant().Code()
			if tenantCode != "" {
				tenant = toPtr(tenantCode)
				if result.Tenant().FromRole() {
					tenantFrom = toPtr("role")
				} else {
					tenantFrom = toPtr("hint")
				}
			}
		}
		if r, err := mctx.GetDbIndex(); err == nil {
			index = toPtr(int(r))
		}
	}

	var row = []*types.Datum{
		createDatumByPrimitivePt(global),
		createDatum(strings.Join(excludes, ",")),
		createDatum(strings.Join(includes, ",")),
		createDatum(types.CreateBinaryJSON(comments)),
		createDatumByPrimitivePt(tenant),
		createDatumByPrimitivePt(tenantFrom),
		createDatum(db),
		createDatumByPrimitivePt(index),
		createDatum(types.CreateBinaryJSON(params)),
		createDatum(restoreSQL),
	}
	e.Rows = append(e.Rows, row)
	return nil
}

func toPtr[T any](v T) *T {
	return &v
}

// 通过pr指针类型构造Datum
func createDatumByPrimitivePt[T any](value *T) *types.Datum {
	d := &types.Datum{}
	if value == nil {
		d.SetNull()
	} else {
		d.SetValueWithDefaultCollation(*value)
	}
	return d
}

// createDatum 通过任意tidb支持的类型构造Datum
func createDatum(value any) *types.Datum {
	d := &types.Datum{}
	if value == nil {
		d.SetNull()
	} else {
		d.SetValueWithDefaultCollation(value)
	}
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

func (e *Execute) getExtensionParams() ([]types.ParamMarkerOffset, int) {
	prepared := e.PrepStmt
	index := slices.IndexFunc(prepared.Params, func(p ast.ParamMarkerExpr) bool {
		return p.(types.ParamMarkerOffset).GetOffset() == mctech.ExtensionParamMarkerOffset
	})

	if index < 0 {
		return nil, -1
	}
	extParams := make([]types.ParamMarkerOffset, 0, len(prepared.Params)-index)
	for _, v := range prepared.Params[index:] {
		extParams = append(extParams, v.(types.ParamMarkerOffset))
	}
	return extParams, index
}

type extensionArgCreator[T any] func() (T, error)

func appendExtensionArgs[T any](
	params []types.ParamMarkerOffset, callback extensionArgCreator[T]) ([]T, error) {
	extensions := []T{}
	for _, p := range params {
		if p.GetOffset() == mctech.ExtensionParamMarkerOffset {
			// 扩展自定义参数
			item, err := callback()
			if err != nil {
				return nil, err
			}
			extensions = append(extensions, item)
		}
	}

	return extensions, nil
}

// AppendVarExprs append custom extension parameters
func (e *Execute) AppendVarExprs(sctx sessionctx.Context) (err error) {
	// 获取扩展参数列表
	extParams, from := e.getExtensionParams()
	if len(extParams) == 0 {
		return nil
	}

	var mctx mctech.Context
	if mctx, err = mctech.GetContext(sctx); err != nil {
		return err
	}

	var (
		tenantValue expression.Expression
		result      mctech.PrepareResult
		tenantCode  string
	)
	// 优先从sql语句中提取租户信息
	if result = mctx.PrepareResult(); result != nil {
		tenantCode = result.Tenant().Code()
		if tenantCode != "" {
			tenantValue = expression.DatumToConstant(types.NewDatum(tenantCode), mysql.TypeString, 0)
		}
	}

	if tenantValue == nil {
		// 其次从参数中提取租户变量
		if len(e.Params) > from {
			tenantValue = e.Params[from]
			if result != nil && tenantValue != nil {
				var (
					code   string
					isNull bool
				)
				if code, isNull, err = tenantValue.EvalString(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{}); err != nil {
					return err
				}

				if !isNull {
					// 提取从参数上获取到的租户code
					tenantCode = code
					result.Tenant().(mctech.MutableTenantInfo).SetCode(code)
				}
			}
			// 暂时先移除参数租户参数
			e.Params = e.Params[:from]
		}
	}

	if tenantValue == nil || tenantCode == "" {
		return errors.New("当前用户无法确定所属租户信息，需要在sql前添加 Hint 提供租户信息。格式为 /*& tenant:'{tenantCode}' */")
	}

	extArgs, err := appendExtensionArgs(extParams, func() (expression.Expression, error) {
		return tenantValue, nil
	})

	if err == nil {
		e.Params = append(e.Params, extArgs...)
	}
	return err
}

// --------------------------------- MCTechLargeQuery --------------------------------------

// MCTechLargeQueryExtractor is used to extract some predicates of `large_sql_query`
type MCTechLargeQueryExtractor struct {
	extractHelper

	SkipRequest bool
	TimeRanges  []*TimeRange
	// Enable is true means the executor should use the time range to locate the large-query-log file that need to be parsed.
	// Enable is false, means the executor should keep the behavior compatible with before, which is only parse the
	// current large-query-log file.
	Enable bool
	Desc   bool
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *MCTechLargeQueryExtractor) Extract(
	ctx PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained, startTime, endTime := e.extractTimeRange(ctx, schema, names, predicates, "time", ctx.GetSessionVars().StmtCtx.TimeZone())
	e.setTimeRange(startTime, endTime)
	e.SkipRequest = e.Enable && e.TimeRanges[0].StartTime.After(e.TimeRanges[0].EndTime)
	if e.SkipRequest {
		return nil
	}
	return remained
}

func (e *MCTechLargeQueryExtractor) explainInfo(p *PhysicalMemTable) string {
	if e.SkipRequest {
		return "skip_request: true"
	}
	if !e.Enable {
		return fmt.Sprintf("only search in the current '%v' file", config.GetMCTechConfig().Metrics.LargeQuery.Filename)
	}
	startTime := e.TimeRanges[0].StartTime.In(p.SCtx().GetSessionVars().StmtCtx.TimeZone())
	endTime := e.TimeRanges[0].EndTime.In(p.SCtx().GetSessionVars().StmtCtx.TimeZone())
	return fmt.Sprintf("start_time:%v, end_time:%v",
		types.NewTime(types.FromGoTime(startTime), mysql.TypeDatetime, types.MaxFsp).String(),
		types.NewTime(types.FromGoTime(endTime), mysql.TypeDatetime, types.MaxFsp).String())
}

func (e *MCTechLargeQueryExtractor) setTimeRange(start, end int64) {
	const defaultLargeQueryDuration = 24 * time.Hour
	var startTime, endTime time.Time
	if start == 0 && end == 0 {
		return
	}
	if start != 0 {
		startTime = e.convertToTime(start)
	}
	if end != 0 {
		endTime = e.convertToTime(end)
	}
	if start == 0 {
		startTime = endTime.Add(-defaultLargeQueryDuration)
	}
	if end == 0 {
		endTime = startTime.Add(defaultLargeQueryDuration)
	}
	timeRange := &TimeRange{
		StartTime: startTime,
		EndTime:   endTime,
	}
	e.TimeRanges = append(e.TimeRanges, timeRange)
	e.Enable = true
}

func (e *MCTechLargeQueryExtractor) buildTimeRangeFromKeyRange(keyRanges []*coprocessor.KeyRange) error {
	for _, kr := range keyRanges {
		startTime, err := e.decodeBytesToTime(kr.Start)
		if err != nil {
			return err
		}
		endTime, err := e.decodeBytesToTime(kr.End)
		if err != nil {
			return err
		}
		e.setTimeRange(startTime, endTime)
	}
	return nil
}

func (e *MCTechLargeQueryExtractor) decodeBytesToTime(bs []byte) (int64, error) {
	if len(bs) >= tablecodec.RecordRowKeyLen {
		t, err := tablecodec.DecodeRowKey(bs)
		if err != nil {
			return 0, nil
		}
		return e.decodeToTime(t)
	}
	return 0, nil
}

func (*MCTechLargeQueryExtractor) decodeToTime(handle kv.Handle) (int64, error) {
	tp := types.NewFieldType(mysql.TypeDatetime)
	col := rowcodec.ColInfo{Ft: tp}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{tp}, 1)
	coder := codec.NewDecoder(chk, nil)
	_, err := coder.DecodeOne(handle.EncodedCol(0), 0, col.Ft)
	if err != nil {
		return 0, err
	}
	datum := chk.GetRow(0).GetDatum(0, tp)
	mysqlTime := (&datum).GetMysqlTime()
	timestampInNano := time.Date(mysqlTime.Year(),
		time.Month(mysqlTime.Month()),
		mysqlTime.Day(),
		mysqlTime.Hour(),
		mysqlTime.Minute(),
		mysqlTime.Second(),
		mysqlTime.Microsecond()*1000,
		time.UTC,
	).UnixNano()
	return timestampInNano, err
}
