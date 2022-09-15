package mctech

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"golang.org/x/exp/slices"
)

// Context mctech context interface
type Context interface {
	// 获取tidb session
	Session() sessionctx.Context

	// @title InPrepareStmt
	// @description 当前请求是否是prepare语句中
	// @return bool
	InPrepareStmt() bool
	// @title CurrentDB
	// @description 当前数据库
	CurrentDB() string
	// @title GetDbIndex
	// @description 获取当前使用的global_dw库的索引号
	GetDbIndex() (DbIndex, error)
	// @title ToPhysicalDbName
	// @description 把给定的库名转换为真实的物理库名。根据传入sql中的dbPrefix添加前缀，如果前缀已存在则什么也不做
	ToPhysicalDbName(db string) (string, error)
	// @title ToLogicDbName
	// @description 把给定的数据库名转换为数据库的逻辑名称。根据传入sql中的dbPrefix，移除前缀，如果前缀不存在则什么也不做
	ToLogicDbName(db string) string
	// @title PrepareResult
	// @description 对sql预处理的结果
	PrepareResult() *PrepareResult
	// @title IsGlobalDb
	// @description 判断给定的库名是否属于global一类的库。（需要考虑是否含有dbPrefix）
	// @param dbName string
	IsGlobalDb(dbName string) bool
	// @title SQLRewrited
	// @description 当前请求中的sql是否已被改动
	SQLRewrited() bool
	// @title SQLHasGlobalDB
	// @description sql中是否包含global类的库
	SQLHasGlobalDB() bool

	// @title UsingTenantParam
	// @description 租户过滤条件是否使用参数
	UsingTenantParam() bool
}

// ContextForTest interface
type ContextForTest interface {
	// @title GetInfoForTest
	// @description 获取用于单元测试的描述信息
	GetInfoForTest() string
}

// ModifyContext interface
type ModifyContext interface {
	// 设置创建租户条件表达式是否使用参数化方式
	SetUsingTenantParam(val bool)
	// @title Reset
	// @description 重置是否改写状态，用于支持一次执行多条sql语句时
	Reset()
	// @title SetPrepareResult
	// @description 设置sql预处理的结果
	// @param result *PrepareResult
	SetPrepareResult(result *PrepareResult)
	// @title SetDBSelector
	// @description 设置DbSelector
	// @param selector DBSelector
	SetDBSelector(selector DBSelector)
	// @title SetSQLRewrited
	// @description 设置sql是否已被改动的标记
	// @param rewrited bool
	SetSQLRewrited(rewrited bool)
	// @title SetSQLHasGlobalDB
	// @description 设置sql中是否包含global类的库
	// @param hasGlobalDB bool
	SetSQLHasGlobalDB(hasGlobalDB bool)
}

// BaseContextAware interface
type BaseContextAware interface {
	BaseContext() Context
}

// DBSelector global_dw_* db index selector
type DBSelector interface {
	GetDbIndex() (DbIndex, error)
}

const (
	// ParamTenant custom hint param "tenant"
	ParamTenant = "tenant"
	// ParamDbPrefix custom hint param "dbPrefix"
	ParamDbPrefix = "dbPrefix"
	// ParamGlobal custom hint param "global"
	ParamGlobal = "global"
)

// GlobalValueInfo from global params
type GlobalValueInfo struct {
	Global   bool
	Excludes []string
}

func (g *GlobalValueInfo) String() string {
	return fmt.Sprintf("{%t,%s}", g.Global, g.Excludes)
}

// PrepareResult sql resolve result
type PrepareResult struct {
	params         map[string]any
	dbPrefix       string
	tenant         string
	globalInfo     *GlobalValueInfo
	tenantFromRole bool
}

// NewPrepareResult create PrepareResult
func NewPrepareResult(tenantCode string, params map[string]any) (*PrepareResult, error) {
	fromRole := tenantCode != ""

	if v, ok := params[ParamTenant]; ok {
		codeFromParam := strings.TrimSpace(v.(string))
		if tenantCode == "" {
			// 如果从角色中无法找到租户信息，并且查询的是global_*库，必须有hint
			// 如果hint写了租户，自动按写的租户补条件
			// 如果sql里也没有找到租户信息，需要等到数据库检查时才能确定当前是否需要租户code信息
			tenantCode = codeFromParam
		} else if codeFromParam != tenantCode {
			// 角色里存在租户信息，sql里也存在租户信息
			// 两个信息不一致时，抛出异常
			return nil, fmt.Errorf("当前用户所属角色对应的租户信息与sql里传入的租户信息不一致. %s (role) <=> %s (sql)", tenantCode, codeFromParam)
		}
	}

	var globalInfo *GlobalValueInfo
	v, ok := params[ParamGlobal]
	if !ok {
		globalInfo = &GlobalValueInfo{}
	} else {
		delete(params, ParamGlobal)
		globalInfo = v.(*GlobalValueInfo)
	}

	if tenantCode != "" && globalInfo.Global {
		return nil, errors.New("存在tenant信息时，global不允许设置为true")
	}

	var dbPrefix string
	if v, ok = params[ParamDbPrefix]; ok {
		dbPrefix = v.(string)
	}

	newParams := make(map[string]any)
	for k, v := range params {
		newParams[k] = v
	}

	r := &PrepareResult{
		tenantFromRole: fromRole,
		tenant:         tenantCode,
		dbPrefix:       dbPrefix,
		globalInfo:     globalInfo,
		params:         newParams,
	}
	return r, nil
}

// String to string
func (r *PrepareResult) String() string {
	lst := make([]string, 0, len(r.params))
	for k, v := range r.params {
		lst = append(lst, fmt.Sprintf("{%s,%s}", k, v))
	}
	slices.Sort(lst)
	return fmt.Sprintf("{%s,%s,%t,%s,%s}",
		r.dbPrefix, r.tenant, r.tenantFromRole, lst, r.globalInfo)
}

// Tenant current tenant
func (r *PrepareResult) Tenant() string {
	return r.tenant
}

// TenantFromRole tenant is from role
func (r *PrepareResult) TenantFromRole() bool {
	return r.tenantFromRole
}

// Global global
func (r *PrepareResult) Global() bool {
	return r.globalInfo.Global
}

// Excludes excludes
func (r *PrepareResult) Excludes() []string {
	return r.globalInfo.Excludes
}

// Params params
func (r *PrepareResult) Params() map[string]any {
	return r.params
}

// DbPrefix db prefix
func (r *PrepareResult) DbPrefix() string {
	return r.dbPrefix
}

type baseContext struct {
	inPrepareStmt    bool
	selector         DBSelector
	prepareResult    *PrepareResult
	sqlRewrited      bool
	sqlHasGlobalDB   bool
	usingTenantParam bool
}

// DbPublicPrefix public类数据库前缀
const DbPublicPrefix = "public_"

// DbAssetPrefix asset类数据库前缀
const DbAssetPrefix = "asset_"

// DbGlobalPrefix global类数据库前缀
const DbGlobalPrefix = "global_"

// DbCustomSuffix 租户自定义数据库后缀
const DbCustomSuffix = "_custom"

// NewBaseContext create mctechContext (Context)
func NewBaseContext(usingTenantParam bool) Context {
	return &baseContext{usingTenantParam: usingTenantParam}
}

func (d *baseContext) CurrentDB() string {
	log.Error("CurrentDB: " + string(debug.Stack()))
	panic(errors.New("CurrentDB not implemented"))
}

func (d *baseContext) PrepareSQL(rawSQL string) (sql string, err error) {
	log.Error("PrepareSQL: " + string(debug.Stack()))
	panic(errors.New("PrepareSQL not implemented"))
}

func (d *baseContext) ApplyAndCheck(stmts []ast.StmtNode) (changed bool, err error) {
	log.Error("ApplyAndCheck: " + string(debug.Stack()))
	panic(errors.New("ApplyAndCheck not implemented"))
}

func (d *baseContext) Session() sessionctx.Context {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("Session not implemented"))
}

// ------------------------------------------------

func (d *baseContext) UsingTenantParam() bool {
	return d.usingTenantParam
}

func (d *baseContext) SetUsingTenantParam(val bool) {
	d.usingTenantParam = val
}

func (d *baseContext) GetInfoForTest() string {
	return fmt.Sprintf("{%s}", d.prepareResult)
}

func (d *baseContext) Reset() {
	d.sqlRewrited = false
	d.sqlHasGlobalDB = false
}

// ------------------------------------------------

func (d *baseContext) SetDBSelector(selector DBSelector) {
	d.selector = selector
}

func (d *baseContext) SetSQLHasGlobalDB(hasGlobalDB bool) {
	d.sqlHasGlobalDB = hasGlobalDB
}

func (d *baseContext) SetPrepareResult(result *PrepareResult) {
	d.prepareResult = result
}

func (d *baseContext) SetSQLRewrited(sqlRewrited bool) {
	d.sqlRewrited = sqlRewrited
}

// ------------------------------------------------

func (d *baseContext) InPrepareStmt() bool {
	return d.inPrepareStmt
}

func (d *baseContext) SQLRewrited() bool {
	return d.sqlRewrited
}

func (d *baseContext) SQLHasGlobalDB() bool {
	return d.sqlHasGlobalDB
}

func (d *baseContext) PrepareResult() *PrepareResult {
	return d.prepareResult
}

func (d *baseContext) ToPhysicalDbName(db string) (string, error) {
	if db == "" {
		return db, nil
	}
	// 处理dw库的索引
	if d.IsGlobalDb(db) && strings.HasSuffix(db, "_dw") {
		dbIndex, err := d.selector.GetDbIndex()
		if err != nil {
			return "", err
		}
		db = fmt.Sprintf("%s_%d", db, dbIndex)
	}

	prefixAvaliable := isProductDatabase(db)
	if !prefixAvaliable {
		// 数据库不支持添加前缀
		return db, nil
	}

	// 到此database支持添加数据库前缀
	result := d.prepareResult
	if result == nil {
		return db, nil
	}
	dbPrefix := result.DbPrefix()

	if dbPrefix == "" {
		return db, nil
	}

	return dbPrefix + "_" + db, nil
}

func (d *baseContext) ToLogicDbName(db string) string {
	if db == "" {
		return db
	}

	result := d.prepareResult
	if result == nil {
		return db
	}

	dbPrefix := result.DbPrefix()
	if dbPrefix == "" || !strings.HasPrefix(db, dbPrefix+"_") {
		return db
	}

	logicDb := db[len(dbPrefix)+1:]
	if isProductDatabase(logicDb) {
		return logicDb
	}
	return db
}

func (d *baseContext) IsGlobalDb(db string) bool {
	result := d.PrepareResult()
	if strings.HasPrefix(db, DbGlobalPrefix) {
		return true
	}

	dbPrefix := result.DbPrefix()
	if dbPrefix != "" {
		return strings.HasPrefix(db, dbPrefix+"_"+DbGlobalPrefix)
	}
	return false
}

func (d *baseContext) GetDbIndex() (DbIndex, error) {
	return d.selector.GetDbIndex()
}

/**
 * 是否属于产品数据库
 *
 * @param logicDbName
 * @return
 */
func isProductDatabase(logicDb string) bool {
	return strings.HasPrefix(logicDb, DbGlobalPrefix) || // global_*是租户相关的
		strings.HasPrefix(logicDb, DbPublicPrefix) || // public_data给将来留的，不花钱的给将来留的，不花钱的
		strings.HasPrefix(logicDb, DbAssetPrefix) // asset_* 是花钱的
}

// DbIndex 表示数据库后缀索引的类型
type DbIndex int

const (
	// FIRST global_dw_*库的序号为1的索引
	FIRST DbIndex = 0x01 //0x01
	// SECOND global_dw_*库的序号为2的索引
	SECOND // 0x02
)

type contextKey struct{}

var customContextKey = contextKey{}

// NewContext function callback
var NewContext func(session sessionctx.Context, usingTenantParam bool) Context

// WithContext
// @Param session sessionctx.Context -
func WithNewContext(session sessionctx.Context) (context.Context, Context) {
	return WithNewContext3(context.Background(), session, false)
}

// WithContext3
// @Param parent context.Context -
// @Param session sessionctx.Context -
// @Param usingTenantParam bool 添加租户条件时，是否使用参数占位符方式
func WithNewContext3(parent context.Context,
	session sessionctx.Context, usingTenantParam bool) (context.Context, Context) {
	mctechCtx := NewContext(session, usingTenantParam)
	ctx := context.WithValue(parent, customContextKey, mctechCtx)
	return ctx, mctechCtx
}

// GetContext get mctech context from session
func GetContext(ctx context.Context) Context {
	val := ctx.Value(customContextKey)
	if sp, ok := val.(Context); ok {
		return sp
	}
	return nil
}

// 添加的租户条件假的文本位置偏移量
const ExtensionParamMarkerOffset = math.MaxInt - 1
