// add by zhangbing

package core

import (
	"context"
	"fmt"
	"reflect"
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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

// MCTechExtensions mctech extension interface to extend PlanCacheStmt struct
type MCTechExtensions interface {
	mctech.ParsedData
	Schema() *mctech.StmtSchemaInfo
}

type mctechExtensions struct {
	parsedResult   mctech.ParseResult
	schema         *mctech.StmtSchemaInfo
	sqlRewrited    bool
	sqlHasGlobalDB bool
}

var (
	_ mctech.ParsedData = &mctechExtensions{}
	_ MCTechExtensions  = &mctechExtensions{}
)

func (e *mctechExtensions) ParseResult() mctech.ParseResult {
	return e.parsedResult
}

func (e *mctechExtensions) Schema() *mctech.StmtSchemaInfo {
	return e.schema
}

func (e *mctechExtensions) SQLRewrited() bool {
	return e.sqlRewrited
}

func (e *mctechExtensions) SQLHasGlobalDB() bool {
	return e.sqlHasGlobalDB
}

// newMCTechExtensions create MCTechExtensions interface instance
func newMCTechExtensions(sctx sessionctx.Context, stmt ast.StmtNode) MCTechExtensions {
	mctx := mctech.GetContextStrict(sctx)
	if mctx == nil && intest.InTest {
		return &mctechExtensions{}
	}
	ext := &mctechExtensions{
		parsedResult:   mctx.ParseResult(),
		sqlRewrited:    mctx.SQLRewrited(),
		sqlHasGlobalDB: mctx.SQLHasGlobalDB(),
	}

	if schema, exists := mctx.GetSchema(stmt); exists {
		ext.schema = &schema
	}
	return ext
}

// fetchMinimumParamCount 压缩多个附加的租户code参数对象为1个对象
// @description 如果存在附加的租户code参数，最终需要调用端提供的参数列表是除了附加的参数外，客外再增加一个租户code参数。如果不存在附加的租户code参数，最终需要调用端提供的参数列表为原始参数列表
func fetchMinimumParamCount(markers []ast.ParamMarkerExpr) int {
	rawCount := 0
	tenantCount := 0
	for _, m := range markers {
		p := m.(types.ParamMarkerOffset)
		if p.GetOffset() == mctech.TenantParamMarkerOffset {
			tenantCount++
		} else {
			rawCount++
		}
	}

	if tenantCount > 0 {
		rawCount++
	}
	return rawCount
}

// ----------------------------------------------------------------------------------------------------

func (b *PlanBuilder) buildMCTech(_ context.Context, stmt *ast.MCTechStmt) (base.Plan, error) {
	p := &MCTech{
		Format:   stmt.Format,
		Stmt:     stmt,
		ExecStmt: stmt.Stmt,
	}
	p.SetSCtx(b.ctx)
	return p, p.prepareSchema()
}

// MCTech represents a mctech plan.
type MCTech struct {
	baseSchemaProducer

	Format   string
	Stmt     ast.StmtNode // mctech 语句本身
	ExecStmt ast.StmtNode // mctech 包含的子语句
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
	{"excludes", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
	{"includes", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
	{"comments", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
	{"tenant", mysql.TypeVarchar, 50},
	{"tenant_from", mysql.TypeVarchar, 10},
	{"db", mysql.TypeVarchar, 50},
	{"dbs", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
	{"tables", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
	{"dw_index", mysql.TypeJSON, getDefaultFieldLength(mysql.TypeJSON)},
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
		comments   mctech.Comments
		tenant     *string
		tenantFrom *string
		params     map[string]any
		db         = e.SCtx().GetSessionVars().CurrentDB
		dbs        []string
		tables     []mctech.TableName
		index      map[string]any
		restoreSQL = sb.String()
	)

	if mctx != nil {
		result := mctx.ParseResult()
		if result != nil {
			global = toPtr(result.TenantOmit())
			params = result.Params()
			excludes = result.Global().Excludes()
			includes = result.Global().Includes()
			if excludes == nil {
				excludes = []string{}
			}
			if includes == nil {
				includes = []string{}
			}
			comments = result.Comments()
			tenantCode := result.Tenant().Code()
			if tenantCode != "" {
				tenant = toPtr(tenantCode)
				if result.Tenant().FromRole() {
					tenantFrom = toPtr("role")
				} else {
					tenantFrom = toPtr("hint")
				}
			}
			info, err := mctx.GetDWIndexInfo()
			if err != nil {
				index = map[string]any{"error": err.Error()}
			} else {
				index = info.ToMap()
			}
		}
		if schema, exists := mctx.GetSchema(e.Stmt); exists {
			dbs = schema.Databases
			tables = schema.Tables
		}
	}

	var row = []*types.Datum{
		createDatumByPrimitivePt(global),
		createDatumByArray(excludes),
		createDatumByArray(includes),
		createDatumByObject(comments),
		createDatumByPrimitivePt(tenant),
		createDatumByPrimitivePt(tenantFrom),
		createDatum(db),
		createDatumByArray(dbs),
		createDatumByArray(tables),
		createDatumByObject(index),
		createDatumByObject(params),
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

func createDatumByArray[T any](value []T) *types.Datum {
	d := &types.Datum{}
	if value == nil {
		d.SetNull()
	} else {
		v := make([]any, 0, len(value))
		for _, el := range value {
			var anyEl any = el
			switch target := anyEl.(type) {
			case string:
				v = append(v, target)
			case mctech.TableName:
				v = append(v, target.ToMap())
			}
		}
		d.SetMysqlJSON(types.CreateBinaryJSON(v))
	}
	return d
}

func createDatumByObject(value any) *types.Datum {
	d := &types.Datum{}
	if value == nil {
		d.SetNull()
	} else {
		if reflect.ValueOf(value).IsNil() {
			d.SetNull()
		} else {
			var v any
			switch target := value.(type) {
			case mctech.Comments:
				v = target.ToMap()
			default:
				v = value
			}
			d.SetMysqlJSON(types.CreateBinaryJSON(v))
		}
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
		return p.(types.ParamMarkerOffset).GetOffset() == mctech.TenantParamMarkerOffset
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
		if p.GetOffset() == mctech.TenantParamMarkerOffset {
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

// AppendVarExprs 扩展租户参数为真实的个数
func (e *Execute) AppendVarExprs(sctx sessionctx.Context) (err error) {
	var mctx mctech.Context
	if mctx, err = mctech.GetContext(sctx); err != nil || mctx == nil {
		return err
	}

	// 替换其中的一部分信息
	extensions := e.PrepStmt.MCTechExtensions
	modifyContext := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
	modifyContext.SetParsedData(extensions)
	if schema := extensions.Schema(); schema != nil {
		modifyContext.SetSchema(e.PrepStmt.PreparedAst.Stmt, *schema)
	}

	// 获取扩展参数列表
	extParams, from := e.getExtensionParams()
	if len(extParams) == 0 {
		return nil
	}

	var (
		tenantValue expression.Expression
		result      mctech.ParseResult
		tenantCode  string
	)

	// 只从第一个扩展参数中提取租户变量，忽略后续其它参数
	if len(e.Params) > from {
		tenantValue = e.Params[from]
		if tenantValue != nil {
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
			}
		}
		// 暂时先移除参数租户参数
		e.Params = e.Params[:from]
	}

	// 与上下文中的租户code比较，判断兼容性
	if result = mctx.ParseResult(); result != nil {
		codeFromCtx := result.Tenant().Code()
		if tenantCode == "" {
			if codeFromCtx != "" {
				// 参数上没有租户code，从context上提取的租户code，生成调用参数
				tenantCode = codeFromCtx
				tenantValue = expression.DatumToConstant(types.NewDatum(tenantCode), mysql.TypeString, 0)
			}
			// else tenantCode == "" && codeFromCtx == ""
		} else {
			if codeFromCtx == "" {
				// 从参数上提取的租户code，赋值到context上
				result.Tenant().(mctech.MutableTenantInfo).SetCode(tenantCode)
			} else {
				// tenantCode != "" && codeFromCtx != ""
				if tenantCode != codeFromCtx {
					return fmt.Errorf("当前用户所属租户信息与传入参数中提取的租户信息不相同. '%s' <==> '%s'", tenantCode, codeFromCtx)
				}
			}
		}
	}

	if tenantValue == nil {
		return errors.New("当前用户无法确定所属租户信息，请确认在参数列表最后额外添加了一个非空的租户code参数")
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
	ctx base.PlanContext,
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

// ExplainInfo implements the Expression interface.
func (e *MCTechLargeQueryExtractor) ExplainInfo(pp base.PhysicalPlan) string {
	p := pp.(*PhysicalMemTable)
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
