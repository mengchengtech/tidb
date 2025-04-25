package mctech

import (
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/intest"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// Context mctech context interface
type Context interface {
	StartedAt() time.Time
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
	// @title SelectDWIndex
	// @description 选择当前环境下计算出的global_dw库的索引号
	SelectDWIndex() (*DWIndex, error)
	// @title GetDWIndexInfo
	// @description 获取global_dw库的全部索引信息
	GetDWIndexInfo() (*DWIndexInfo, error)
	// @title ToPhysicalDbName
	// @description 把给定的库名转换为真实的物理库名。根据传入sql中的dbPrefix添加前缀，如果前缀已存在则什么也不做
	ToPhysicalDbName(db string) (string, error)
	// @title ToLogicDbName
	// @description 把给定的数据库名转换为数据库的逻辑名称。根据传入sql中的dbPrefix，移除前缀，如果前缀不存在则什么也不做
	ToLogicDbName(db string) string
	// @title PrepareResult
	// @description 对sql预处理的结果
	PrepareResult() PrepareResult
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
	// 是否是在execute语句中运行
	InExecute() bool
	// 获取给定sql语法树里用到的数据库
	GetDbs(stmt ast.StmtNode) []string
	// 设置给定sql语法树里用到的数据库
	SetDbs(stmt ast.StmtNode, dbs []string)
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
	// 设置是否是在Execute中运行
	SetInExecute(val bool)
	// @title Reset
	// @description 重置是否改写状态，用于支持一次执行多条sql语句时
	Reset()
	// @title SetPrepareResult
	// @description 设置sql预处理的结果
	// @param result PrepareResult
	SetPrepareResult(result PrepareResult)
	// @title SetDWSelector
	// @description 设置DWSelector
	// @param selector DWSelector
	SetDWSelector(selector DWSelector)
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
	StoreSessionMPPVars(mpp string) error
	ReloadSessionMPPVars() error
	SetSessionMPPVars(mpp string) error
}

// BaseContextAware interface
type BaseContextAware interface {
	BaseContext() Context
}

// DWSelector global_dw_* db index selector
type DWSelector interface {
	SelectIndex(dbPrefix, requestID string, forcebackground bool) (*DWIndex, error)
	GetIndexInfo(dbPrefix string) (*DWIndexInfo, error)
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

	// CommentFrom 执行sql的服务
	CommentFrom = "from"
	// CommentPackage 执行sql的依赖包
	CommentPackage = "package"
)

// GlobalValueInfo from global params
type GlobalValueInfo interface {
	// Global global
	Global() bool
	// Excludes excludes
	Excludes() []string
	// Includes includes
	Includes() []string
}

// NewGlobalValue create GlobalValueInfo instance
func NewGlobalValue(global bool, excludes, includes []string) GlobalValueInfo {
	return &globalValueInfo{
		global:   global,
		excludes: excludes,
		includes: includes,
	}
}

type globalValueInfo struct {
	global   bool
	excludes []string
	includes []string
}

func (g *globalValueInfo) Global() bool {
	return g.global
}

func (g *globalValueInfo) Excludes() []string {
	return g.excludes
}

func (g *globalValueInfo) Includes() []string {
	return g.includes
}

func (g *globalValueInfo) String() string {
	return fmt.Sprintf("{%t,%v,%v}", g.global, g.excludes, g.includes)
}

// FlagRoles custom roles
type FlagRoles interface {
	TenantOmit() bool // 是否包含tenant_omit角色。跳过租户隔离，用于一些需要同步数据的特殊场景
	TenantOnly() bool // 是否包含tenant_only 角色。限制必须存在租户隔离
	AcrossDB() bool   // 是否包含 across_db 角色。保留字段，暂时没有使用
}

// Comments sql中特殊的注释信息
type Comments interface {
	Service() ServiceComment // 执行sql的服务名称
	Package() PackageComment // 执行sql所属的依赖包名称（公共包中执行的sql）
	String() string
}

// ServiceComment service comment
type ServiceComment interface {
	From() string        // {appName}[.{productLine}]
	AppName() string     // 执行sql的服务名称
	ProductLine() string // 执行sql所属的服务的产品线
}

// PackageComment package comment
type PackageComment interface {
	Name() string
}

// TenantInfo tenant info
type TenantInfo interface {
	fmt.Stringer
	Code() string   // 当前租户code
	FromRole() bool // 租户code是否来自角色
}

// MutableTenantInfo tenant info
type MutableTenantInfo interface {
	SetCode(string) // 当前租户code
}

// tenantValueInfo tenant info
type tenantValueInfo struct {
	code     string // 当前租户code
	fromRole bool   // 租户code是否来自角色
}

// Code tenant code
func (ti *tenantValueInfo) Code() string {
	return ti.code
}

func (ti *tenantValueInfo) SetCode(code string) {
	ti.code = code
}

// FromRole tenant code is from role?
func (ti *tenantValueInfo) FromRole() bool {
	return ti.fromRole
}

func (ti *tenantValueInfo) String() string {
	return fmt.Sprintf("{%s,%t}", ti.code, ti.fromRole)
}

// PrepareResult interface
type PrepareResult interface {
	fmt.Stringer
	// Tenant current tenant
	Tenant() TenantInfo
	// Roles current user has some roles
	Roles() FlagRoles
	// TenantOmit tenant omit include global & omit
	TenantOmit() bool
	// Comments custom comment
	Comments() Comments
	// Global global
	Global() GlobalValueInfo
	// Params params
	Params() map[string]any
	// DbPrefix 自定义hint，数据库前缀。'dev', 'test'
	// Deprecated: 已废弃
	DbPrefix() string
}

type prepareResult struct {
	// Deprecated: 已废弃
	dbPrefix   string          // 自定义hint，数据库前缀。'dev', 'test'
	params     map[string]any  // 自定义hint的一般参数
	globalInfo GlobalValueInfo // global hint 解析后的值
	tenant     TenantInfo      // 当前sql执行时的租户信息
	roles      FlagRoles       // 当前执行账号的特殊角色信息
	comments   Comments        // 从特殊注释中提取的一些信息
}

// NewPrepareResult create PrepareResult
func NewPrepareResult(tenantCode string, roles FlagRoles, comments Comments, params map[string]any) (PrepareResult, error) {
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

	var globalInfo GlobalValueInfo
	v, ok := params[ParamGlobal]
	if !ok {
		globalInfo = NewGlobalValue(false, nil, nil)
	} else {
		delete(params, ParamGlobal)
		globalInfo = v.(GlobalValueInfo)
	}

	if tenantCode != "" && globalInfo.Global() {
		return nil, errors.New("存在tenant信息时，global不允许设置为true")
	}

	if roles.TenantOmit() {
		// 存在忽略租户的角色，租户code设置为""
		tenantCode = ""
		fromRole = false
	}

	var dbPrefix string
	if v, ok = params[ParamDbPrefix]; ok {
		dbPrefix = v.(string)
	}

	newParams := make(map[string]any)
	for k, v := range params {
		newParams[k] = v
	}

	r := &prepareResult{
		comments: comments,
		roles:    roles,
		tenant: &tenantValueInfo{
			code:     tenantCode,
			fromRole: fromRole,
		},
		dbPrefix:   dbPrefix,
		globalInfo: globalInfo,
		params:     newParams,
	}
	return r, nil
}

// String to string
func (r *prepareResult) String() string {
	var paramList []string
	if len(r.params) > 0 {
		paramList = make([]string, 0, len(r.params))
		keys := maps.Keys(r.params)
		slices.Sort(keys)
		for _, k := range keys {
			paramList = append(paramList, fmt.Sprintf("{%s,%s}", k, r.params[k]))
		}
	}
	return fmt.Sprintf("%s{%s,%v,%s,%s}", r.comments, r.dbPrefix, r.tenant, paramList, r.globalInfo)
}

// Tenant current tenant
func (r *prepareResult) Tenant() TenantInfo {
	return r.tenant
}

// Roles current user has some roles
func (r *prepareResult) Roles() FlagRoles {
	return r.roles
}

// TenantOmit tenant omit
func (r *prepareResult) TenantOmit() bool {
	return r.globalInfo.Global() || r.roles.TenantOmit()
}

// Comments custom comment
func (r *prepareResult) Comments() Comments {
	return r.comments
}

// Global global
func (r *prepareResult) Global() GlobalValueInfo {
	return r.globalInfo
}

// Params params
func (r *prepareResult) Params() map[string]any {
	return r.params
}

// DbPrefix 自定义hint，数据库前缀。'dev', 'test'
// Deprecated: 已废弃
func (r *prepareResult) DbPrefix() string {
	return r.dbPrefix
}

type baseContext struct {
	startedAt        time.Time
	inPrepareStmt    bool
	selector         DWSelector
	prepareResult    PrepareResult
	sqlRewrited      bool
	sqlHasGlobalDB   bool
	usingTenantParam bool
	inExecute        bool
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
	return &baseContext{
		startedAt:        time.Now(),
		usingTenantParam: usingTenantParam,
		dbsDict:          make(map[ast.StmtNode][]string),
	}
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

func (d *baseContext) StoreSessionMPPVars(mpp string) error {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[StoreSessionMPPVars] not implemented"))
}

func (d *baseContext) ReloadSessionMPPVars() error {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[ReloadSessionMPPVars] not implemented"))
}

func (d *baseContext) SetSessionMPPVars(mpp string) error {
	log.Error("Session: " + string(debug.Stack()))
	panic(errors.New("[SetSessionMPPVars] not implemented"))
}

// ------------------------------------------------

func (d *baseContext) StartedAt() time.Time {
	failpoint.Inject("StartedAt", func(v failpoint.Value) {
		str := v.(string)
		if t, err := time.ParseInLocation("2006-01-02 15:04:05.000", str, time.Local); err == nil {
			failpoint.Return(t)
		} else {
			panic(err)
		}
	})
	return d.startedAt
}

func (d *baseContext) UsingTenantParam() bool {
	return d.usingTenantParam
}

func (d *baseContext) InExecute() bool {
	return d.inExecute
}

func (d *baseContext) SetUsingTenantParam(val bool) {
	d.usingTenantParam = val
}

func (d *baseContext) SetInExecute(val bool) {
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

func (d *baseContext) SetDWSelector(selector DWSelector) {
	d.selector = selector
}

func (d *baseContext) SetSQLHasGlobalDB(hasGlobalDB bool) {
	d.sqlHasGlobalDB = hasGlobalDB
}

func (d *baseContext) SetPrepareResult(result PrepareResult) {
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

func (d *baseContext) PrepareResult() PrepareResult {
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
		dwIndex, err := d.SelectDWIndex()
		if err != nil {
			return "", err
		}
		db = fmt.Sprintf("%s_%d", db, *dwIndex)
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

const paramBackgroundKey = "background"
const paramRequestIDKey = "requestId"

func (d *baseContext) SelectDWIndex() (*DWIndex, error) {
	sel := d.selector
	if sel == nil {
		return nil, errors.New("db selector is nil")
	}

	result := d.prepareResult
	params := result.Params()
	dbPrefix := result.DbPrefix()
	var requestID string
	_, forceBackground := params[paramBackgroundKey]
	if !forceBackground {
		if value, ok := params[paramRequestIDKey]; ok {
			requestID = value.(string)
		}
	}
	return sel.SelectIndex(dbPrefix, requestID, forceBackground)
}

func (d *baseContext) GetDWIndexInfo() (*DWIndexInfo, error) {
	sel := d.selector
	if sel == nil {
		return nil, errors.New("db selector is nil")
	}
	return sel.GetIndexInfo(d.prepareResult.DbPrefix())
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

// DWIndex 表示数据库后缀索引的类型
type DWIndex int

const (
	// DWIndexFirst global_dw_*库的序号为1的索引
	DWIndexFirst DWIndex = 1
	// DWIndexSecond global_dw_*库的序号为2的索引
	DWIndexSecond DWIndex = 2
)

// DWIndexInfo 数据仓库前后台库信息
type DWIndexInfo struct {
	Current    DWIndex // 当前正在使用的库索引
	Background DWIndex // 后台库，用于预生成数据
}

func (info *DWIndexInfo) ToMap() map[string]any {
	return map[string]any{
		"current":    int64(info.Current),
		"background": int64(info.Background),
	}
}

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
	if mctx, err := GetContext(sctx); err != nil {
		panic(err)
	} else {
		return mctx
	}
}
