package mctech

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"runtime/debug"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Context mctech context interface
type Context interface {
	// 获取tidb session
	Session() sessionctx.Context
	// 清除session中添加的自定义变量
	Clear()

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

	// 获取给定sql语法树里用到的数据库
	GetDbs(stmt ast.StmtNode) []string
	// 设置给定sql语法树里用到的数据库
	SetDbs(stmt ast.StmtNode, dbs []string)
}

// ContextForTest interface
type ContextForTest interface {
	// @title GetInfoForTest
	// @description 获取用于单元测试的描述信息
	GetInfoForTest() map[string]any
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

// SessionMPPVarsContext interface
type SessionMPPVarsContext interface {
	StoreSessionMPPVars(mpp string) (err error)

	ReloadSessionMPPVars() (err error)

	SetSessionMPPVars(mpp string) (err error)
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
	// ParamTenant 自定义hint，租户限制条件
	ParamTenant = "tenant"
	// ParamDbPrefix 自定义hint，数据库前缀。'dev', 'test'
	// Deprecated: 已废弃
	ParamDbPrefix = "dbPrefix"
	// ParamGlobal 自定义hint，忽略租户隔离条件，使用全局查询
	ParamGlobal = "global"
	// ParamMPP 自定义hint，当前sql执行过程配置的mpp开关项
	ParamMPP = "mpp"
	// ParamAcross 自定义hint，额外允许跨库查询的数据库
	ParamAcross = "across"
	// ParamImpersonate 自定义hint，模拟特殊角色的功能
	ParamImpersonate = "impersonate"
)

// GlobalValueInfo from global params
type GlobalValueInfo struct {
	Global   bool
	Excludes []string
}

// GetInfoForTest get info for test
func (g *GlobalValueInfo) GetInfoForTest() map[string]any {
	info := map[string]any{"set": g.Global}
	if len(g.Excludes) > 0 {
		info["excludes"] = g.Excludes
	}
	return info
}

// PrepareResult sql resolve result
type PrepareResult struct {
	params map[string]any
	// 自定义hint，数据库前缀。'dev', 'test'
	// Deprecated: 已废弃
	dbPrefix       string
	tenant         string
	globalInfo     *GlobalValueInfo
	tenantFromRole bool
	tenantOnlyRole bool
}

// NewPrepareResult create PrepareResult
func NewPrepareResult(tenantCode string, tenantOnly bool, params map[string]any) (*PrepareResult, error) {
	fromRole := tenantCode != ""
	if _, ok := params[ParamMPP]; !ok {
		params[ParamMPP] = config.GetMCTechConfig().MPP.DefaultValue
	}

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
		tenantOnlyRole: tenantOnly,
		tenant:         tenantCode,
		dbPrefix:       dbPrefix,
		globalInfo:     globalInfo,
		params:         newParams,
	}
	return r, nil
}

// GetInfoForTest get info for test
func (r *PrepareResult) GetInfoForTest() map[string]any {
	info := map[string]any{}
	if len(r.params) > 0 {
		info["params"] = maps.Clone(r.params)
	}
	if len(r.dbPrefix) > 0 {
		info["prefix"] = r.dbPrefix
	}
	if r.tenantFromRole {
		info["tenantFromRole"] = r.tenantFromRole
	}
	if len(r.tenant) > 0 {
		info["tenant"] = r.tenant
	}
	if r.globalInfo.Global {
		info["global"] = r.globalInfo.GetInfoForTest()
	}
	return info
}

// Tenant current tenant
func (r *PrepareResult) Tenant() string {
	return r.tenant
}

// TenantFromRole tenant is from role
func (r *PrepareResult) TenantFromRole() bool {
	return r.tenantFromRole
}

// TenantOnlyRole current user has role 'tenant_only'
func (r *PrepareResult) TenantOnlyRole() bool {
	return r.tenantOnlyRole
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

// DbPrefix 自定义hint，数据库前缀。'dev', 'test'
// Deprecated: 已废弃
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
	dbsDict          map[ast.StmtNode][]string
}

var (
	_ Context = &baseContext{}
)

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
	return &baseContext{usingTenantParam: usingTenantParam, dbsDict: make(map[ast.StmtNode][]string)}
}

func (d *baseContext) CurrentDB() string {
	log.Error("CurrentDB: " + string(debug.Stack()))
	panic(errors.New("[CurrentDB] not implemented"))
}

func (d *baseContext) Session() sessionctx.Context {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[Session] not implemented"))
}

func (d *baseContext) Clear() {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[Clear] not implemented"))
}

// ------------------------------------------------

func (d *baseContext) StoreSessionMPPVars(mpp string) (err error) {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[StoreSessionMPPVars] not implemented"))
}

func (d *baseContext) ReloadSessionMPPVars() (err error) {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[ReloadSessionMPPVars] not implemented"))
}

func (d *baseContext) SetSessionMPPVars(mpp string) (err error) {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[SetSessionMPPVars] not implemented"))
}

// ------------------------------------------------

func (d *baseContext) UsingTenantParam() bool {
	return d.usingTenantParam
}

func (d *baseContext) SetUsingTenantParam(val bool) {
	d.usingTenantParam = val
}

func (d *baseContext) GetInfoForTest() map[string]any {
	return d.prepareResult.GetInfoForTest()
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
	result := d.prepareResult
	if result == nil {
		return db, nil
	}
	// 处理dw库的索引
	if d.IsGlobalDb(db) && strings.HasSuffix(db, "_dw") {
		dbIndex, err := d.GetDbIndex()
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
	if strings.HasPrefix(db, DbGlobalPrefix) {
		return true
	}

	dbPrefix := ""
	result := d.PrepareResult()
	if result != nil {
		dbPrefix = result.DbPrefix()
	}
	if dbPrefix != "" {
		return strings.HasPrefix(db, dbPrefix+"_"+DbGlobalPrefix)
	}
	return false
}

func (d *baseContext) GetDbIndex() (DbIndex, error) {
	sel := d.selector
	if sel == nil {
		return -1, errors.New("db selector is nil")
	}
	return sel.GetDbIndex()
}

func (d *baseContext) GetDbs(stmt ast.StmtNode) []string {
	if dbs, ok := d.dbsDict[stmt]; ok {
		return dbs
	}
	return nil
}

func (d *baseContext) SetDbs(stmt ast.StmtNode, dbs []string) {
	d.dbsDict[stmt] = dbs
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

// NewContext function callback
var NewContext func(sctx sessionctx.Context, usingTenantParam bool) Context

// WithNewContext create mctech.Context
// @Param sctx sessionctx.Context -
func WithNewContext(sctx sessionctx.Context) (Context, error) {
	if NewContext == nil {
		var err error
		if !intest.InTest {
			err = errors.New("function variable 'mctech.NewContext' is nil")
		}
		return nil, err
	}

	mctx := NewContext(sctx, false)
	sctx.SetValue(MCContextVarKey, mctx)
	return mctx, nil
}

var errContextNotExists = errors.New("CAN NOT Found 'mctech.Context'")

// GetContext get mctech context from session
func GetContext(sctx sessionctx.Context) (Context, error) {
	val := sctx.Value(MCContextVarKey)
	if sp, ok := val.(Context); ok {
		return sp, nil
	}

	if intest.InTest {
		failpoint.Inject("EnsureContext", func() {
			failpoint.Return(nil, errContextNotExists)
		})
		return nil, nil
	}

	return nil, errContextNotExists
}

// GetContextStrict get mctech context from session
func GetContextStrict(sctx sessionctx.Context) Context {
	mctx, err := GetContext(sctx)
	if err != nil {
		panic(err)
	}
	return mctx
}

// ExtensionParamMarkerOffset 添加的租户条件假的文本位置偏移量
const ExtensionParamMarkerOffset = math.MaxInt - 1

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
