package mctech

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/sessionctx"
	"golang.org/x/exp/slices"
)

// Context mctech context interface
type Context interface {
	CurrentDB() string // 当前数据库
	Reset()
	GetDbIndex() (DbIndex, error)
	ToPhysicalDbName(db string) (string, error) // 转换为物理库名称
	ToLogicDbName(db string) string             // 转换为数据库的逻辑名称
	PrapareResult() *PrapareResult
	SetResolveResult(result *PrapareResult)
	IsGlobalDb(dbName string) bool
	SQLRewrited() bool
	SetSQLRewrited(rewrited bool)
	SQLWithGlobalPrefixDB() bool
	SetSQLWithGlobalPrefixDB(sqlWithGlobalPrefixDB bool)
	GetInfo() string
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

// PrapareResult sql resolve result
type PrapareResult struct {
	params         map[string]any
	dbPrefix       string
	tenant         string
	globalInfo     *GlobalValueInfo
	tenantFromRole bool
}

// NewPrapareResult create PrapareResult
func NewPrapareResult(tenant string, params map[string]any) (*PrapareResult, error) {
	fromRole := tenant != ""
	if !fromRole {
		// 如果从角色中无法找到租户信息，并且查询的是global库，必须有hint，如果没有报错，如果hint写了租户，自动按写的租户补条件
		// 使用hint中提取的租户信息
		// 角色里没有找到租户信息，需要等到数据库检查时才能确定当前是否需要租户code信息
		if v, ok := params[ParamTenant]; ok {
			tenant = strings.TrimSpace(v.(string))
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

	if tenant != "" && globalInfo.Global {
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

	r := &PrapareResult{
		tenantFromRole: fromRole,
		tenant:         tenant,
		dbPrefix:       dbPrefix,
		globalInfo:     globalInfo,
		params:         newParams,
	}
	return r, nil
}

// String to string
func (r *PrapareResult) String() string {
	lst := make([]string, 0, len(r.params))
	for k, v := range r.params {
		lst = append(lst, fmt.Sprintf("{%s,%s}", k, v))
	}
	slices.Sort(lst)
	return fmt.Sprintf("{%s,%s,%t,%s,%s}",
		r.dbPrefix, r.tenant, r.tenantFromRole, lst, r.globalInfo)
}

// Tenant current tenant
func (r *PrapareResult) Tenant() string {
	return r.tenant
}

// TenantFromRole tenant is from role
func (r *PrapareResult) TenantFromRole() bool {
	return r.tenantFromRole
}

// Global global
func (r *PrapareResult) Global() bool {
	return r.globalInfo.Global
}

// Excludes excludes
func (r *PrapareResult) Excludes() []string {
	return r.globalInfo.Excludes
}

// Params params
func (r *PrapareResult) Params() map[string]any {
	return r.params
}

// DbPrefix db prefix
func (r *PrapareResult) DbPrefix() string {
	return r.dbPrefix
}

type mctechContext struct {
	selector              DBSelector
	prapareResult         *PrapareResult
	sqlRewrited           bool
	sqlWithGlobalPrefixDB bool
}

const DbPublicPrefix = "public_"
const DbAssetPrefix = "asset_"
const DbGlobalPrefix = "global_"

// NewBaseContext create mctechContext (Context)
func NewBaseContext(prapareResult *PrapareResult, dbSelector DBSelector) Context {
	return &mctechContext{
		prapareResult: prapareResult,
		selector:      dbSelector,
	}
}

func (d *mctechContext) GetInfo() string {
	return fmt.Sprintf("{%s}", d.prapareResult)
}

func (d *mctechContext) CurrentDB() string {
	panic(errors.New("not implemented"))
}

func (d *mctechContext) Reset() {
	d.sqlRewrited = false
	d.sqlWithGlobalPrefixDB = false
}

func (d *mctechContext) SQLRewrited() bool {
	return d.sqlRewrited
}

func (d *mctechContext) SetSQLRewrited(sqlRewrited bool) {
	d.sqlRewrited = sqlRewrited
}

func (d *mctechContext) SQLWithGlobalPrefixDB() bool {
	return d.sqlWithGlobalPrefixDB
}

func (d *mctechContext) SetSQLWithGlobalPrefixDB(sqlWithGlobalPrefixDB bool) {
	d.sqlWithGlobalPrefixDB = sqlWithGlobalPrefixDB
}

func (d *mctechContext) PrapareResult() *PrapareResult {
	return d.prapareResult
}

func (d *mctechContext) SetResolveResult(result *PrapareResult) {
	d.prapareResult = result
}

func (d *mctechContext) ToPhysicalDbName(db string) (string, error) {
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
	result := d.prapareResult
	if result == nil {
		return db, nil
	}
	dbPrefix := result.DbPrefix()

	if dbPrefix == "" {
		return db, nil
	}

	return dbPrefix + "_" + db, nil
}

func (d *mctechContext) ToLogicDbName(db string) string {
	if db == "" {
		return db
	}

	result := d.prapareResult
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

func (d *mctechContext) IsGlobalDb(db string) bool {
	result := d.PrapareResult()
	if strings.HasPrefix(db, DbGlobalPrefix) {
		return true
	}

	dbPrefix := result.DbPrefix()
	if dbPrefix != "" {
		return strings.HasPrefix(db, dbPrefix+"_"+DbGlobalPrefix)
	}
	return false
}

func (d *mctechContext) GetDbIndex() (DbIndex, error) {
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

type sessionValueKey string

func (s sessionValueKey) String() string {
	return string(s)
}

const contextKey sessionValueKey = "$$MCTechContext"

// GetContext get mctech context from session
func GetContext(s sessionctx.Context) Context {
	if ctx, ok := s.Value(contextKey).(Context); ok {
		return ctx
	}
	return nil
}

// SetContext set mctech context to session
func SetContext(s sessionctx.Context, ctx Context) {
	s.SetValue(contextKey, ctx)
}
