package mctech

import (
	"errors"
	"fmt"
	"maps"
	"strings"
)

type MCTechContext interface {
	CurrentDB() string // 当前数据库
	Reset()
	ToPhysicalDbName(db string) string // 转换为物理库名称
	ToLogicDbName(db string) string    // 转换为数据库的逻辑名称
	ResolveResult() *ResolveResult
	SetResolveResult(result *ResolveResult)
	IsGlobalDb(dbName string) bool
	SqlRewrited() bool
	SetSqlRewrited(rewrited bool)
	SqlWithGlobalPrefixDB() bool
	SetSqlWithGlobalPrefixDB(sqlWithGlobalPrefixDB bool)
	GetInfoForTest() map[string]any
}

type DBSelector interface {
	GetDbIndex() (DbIndex, error)
}

const PARAM_TENANT = "tenant"
const PARAM_DB_PREFIX = "dbPrefix"
const PARAM_GLOBAL = "global"

type GlobalValueInfo struct {
	Global   bool
	Excludes []string
}

func NewGlobalValueInfo(global bool, excludes ...string) *GlobalValueInfo {
	return &GlobalValueInfo{Global: global, Excludes: excludes}
}

func (g *GlobalValueInfo) GetInfoForTest() map[string]any {
	info := map[string]any{"set": g.Global}
	if len(g.Excludes) > 0 {
		info["excludes"] = g.Excludes
	}
	return info
}

type ResolveResult struct {
	params         map[string]any
	dbPrefix       string
	tenant         string
	globalInfo     *GlobalValueInfo
	tenantFromRole bool
}

func NewResolveResult(tenant string, params map[string]any) (*ResolveResult, error) {
	fromRole := tenant != ""
	if !fromRole {
		// 如果从角色中无法找到租户信息，并且查询的是global库，必须有hint，如果没有报错，如果hint写了租户，自动按写的租户补条件
		// 使用hint中提取的租户信息
		// 角色里没有找到租户信息，需要等到数据库检查时才能确定当前是否需要租户code信息
		if v, ok := params[PARAM_TENANT]; ok {
			tenant = strings.TrimSpace(v.(string))
		}
	}

	var globalInfo *GlobalValueInfo
	v, ok := params[PARAM_GLOBAL]
	if !ok {
		globalInfo = &GlobalValueInfo{}
	} else {
		delete(params, PARAM_GLOBAL)
		globalInfo = v.(*GlobalValueInfo)
	}

	if tenant != "" && globalInfo.Global {
		return nil, errors.New("存在tenant信息时，global不允许设置为true")
	}

	var dbPrefix string
	if v, ok = params[PARAM_DB_PREFIX]; ok {
		dbPrefix = v.(string)
	}

	newParams := make(map[string]any)
	for k, v := range params {
		newParams[k] = v
	}

	r := &ResolveResult{
		tenantFromRole: fromRole,
		tenant:         tenant,
		dbPrefix:       dbPrefix,
		globalInfo:     globalInfo,
		params:         newParams,
	}
	return r, nil
}

func (r *ResolveResult) GetInfoForTest() map[string]any {
	info := map[string]any{}
	if len(r.params) > 0 {
		info["params"] = maps.Clone(r.params)
	}
	if len(r.dbPrefix) > 0 {
		info["prefix"] = r.dbPrefix
	}
	if r.tenantFromRole {
		info["role"] = r.tenantFromRole
	}
	if len(r.tenant) > 0 {
		info["tenant"] = r.tenant
	}
	if r.globalInfo.Global {
		info["global"] = r.globalInfo.GetInfoForTest()
	}
	return info
}

func (r *ResolveResult) Tenant() string {
	return r.tenant
}

func (r *ResolveResult) TenantFromRole() bool {
	return r.tenantFromRole
}

func (r *ResolveResult) Global() bool {
	return r.globalInfo.Global
}

func (r *ResolveResult) Excludes() []string {
	return r.globalInfo.Excludes
}

func (r *ResolveResult) Params() map[string]any {
	return r.params
}

func (r *ResolveResult) DbPrefix() string {
	return r.dbPrefix
}

type baseMCTechContext struct {
	selector              DBSelector
	resolveResult         *ResolveResult
	sqlRewrited           bool
	sqlWithGlobalPrefixDB bool
}

const DB_PUBLIC_DATA = "public_data"
const DB_ASSET_PREFIX = "asset_"
const DB_GLOBAL_PREFIX = "global_"

func NewBaseMCTechContext(resolveResult *ResolveResult, dbSelector DBSelector) MCTechContext {
	return &baseMCTechContext{
		resolveResult: resolveResult,
		selector:      dbSelector,
	}
}

func (d *baseMCTechContext) GetInfoForTest() map[string]any {
	return d.resolveResult.GetInfoForTest()
}

func (d *baseMCTechContext) CurrentDB() string {
	panic(errors.New("not implemented"))
}

func (d *baseMCTechContext) Reset() {
	d.sqlRewrited = false
	d.sqlWithGlobalPrefixDB = false
}

func (d *baseMCTechContext) SqlRewrited() bool {
	return d.sqlRewrited
}

func (d *baseMCTechContext) SetSqlRewrited(sqlRewrited bool) {
	d.sqlRewrited = sqlRewrited
}

func (d *baseMCTechContext) SqlWithGlobalPrefixDB() bool {
	return d.sqlWithGlobalPrefixDB
}

func (d *baseMCTechContext) SetSqlWithGlobalPrefixDB(sqlWithGlobalPrefixDB bool) {
	d.sqlWithGlobalPrefixDB = sqlWithGlobalPrefixDB
}

func (d *baseMCTechContext) ResolveResult() *ResolveResult {
	return d.resolveResult
}

func (d *baseMCTechContext) SetResolveResult(result *ResolveResult) {
	d.resolveResult = result
}

func (d *baseMCTechContext) ToPhysicalDbName(db string) string {
	if db == "" {
		return db
	}
	// 处理dw库的索引
	if d.IsGlobalDb(db) && strings.HasSuffix(db, "_dw") {
		dbIndex, err := d.selector.GetDbIndex()
		if err != nil {
			panic(err)
		}
		db = fmt.Sprintf("%s_%d", db, dbIndex)
	}

	prefixAvaliable := isProductDatabase(db)
	if !prefixAvaliable {
		// 数据库不支持添加前缀
		return db
	}

	// 到此database支持添加数据库前缀
	result := d.resolveResult
	if result == nil {
		return db
	}
	dbPrefix := result.DbPrefix()

	if dbPrefix == "" {
		return db
	}

	return dbPrefix + "_" + db
}

func (d *baseMCTechContext) ToLogicDbName(db string) string {
	if db == "" {
		return db
	}

	result := d.resolveResult
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

func (d *baseMCTechContext) IsGlobalDb(db string) bool {
	result := d.ResolveResult()
	if strings.HasPrefix(db, DB_GLOBAL_PREFIX) {
		return true
	}

	dbPrefix := result.DbPrefix()
	if dbPrefix != "" {
		return strings.HasPrefix(db, dbPrefix+"_"+DB_GLOBAL_PREFIX)
	}
	return false
}

/**
 * 是否属于产品数据库
 *
 * @param logicDbName
 * @return
 */
func isProductDatabase(logicDb string) bool {
	return strings.HasPrefix(logicDb, DB_GLOBAL_PREFIX) || // global_*是租户相关的
		DB_PUBLIC_DATA == logicDb || // public_data给将来留的，不花钱的
		strings.HasPrefix(logicDb, DB_ASSET_PREFIX) // asset_* 是花钱的
}

type DbIndex int

const (
	FIRST  DbIndex = 0x01 //0x01
	SECOND                // 0x02
)
