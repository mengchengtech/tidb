package prapared

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	goCache "github.com/patrickmn/go-cache"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/sessionctx"
)

type tidbSessionMCTechContext struct {
	mctech.MCTechContext
	session sessionctx.Context
}

func NewMCTechContext(
	ctx sessionctx.Context,
	resolveResult *mctech.ResolveResult,
	dbdbSelector mctech.DBSelector) mctech.MCTechContext {
	return &tidbSessionMCTechContext{
		MCTechContext: mctech.NewBaseMCTechContext(resolveResult, dbdbSelector),
		session:       ctx,
	}
}

func (d *tidbSessionMCTechContext) CurrentDB() string {
	return d.session.GetSessionVars().CurrentDB
}

func (d *tidbSessionMCTechContext) GetInfo() string {
	info := d.MCTechContext.GetInfo()
	return fmt.Sprintf("{%s,%s}", info, d.CurrentDB())
}

const paramBackgroundKey = "background"
const paramRequestIdKey = "requestId"

// TODO: 暂时留下的占位参数值
const localCacheKey = "global"

type dbSelector struct {
	// private final static URI BASE_URI;
	resolveResult *mctech.ResolveResult
	ticketMap     *goCache.Cache
	currentMap    *goCache.Cache
}

func newDBSelector(resolveResult *mctech.ResolveResult) mctech.DBSelector {
	return &dbSelector{
		resolveResult: resolveResult,
		ticketMap:     goCache.New(60*time.Second, 20*time.Second),
		currentMap:    goCache.New(15*time.Second, 10*time.Second),
	}
}

func (d *dbSelector) GetDbIndex() (mctech.DbIndex, error) {
	result := d.resolveResult
	params := result.Params()
	env := result.DbPrefix()
	var dbIndex mctech.DbIndex = -1
	var err error
	_, forceBackground := params[paramBackgroundKey]
	if forceBackground {
		// 强制使用后台库
		dbIndex, err = d.forceBackground(env)
	} else {
		if ticket, ok := params[paramRequestIdKey]; ok {
			// 同一个ticket使用相同的后端库
			dbIndex, err = d.getDbIndexByTicket(env, ticket.(string))
		}
	}

	if dbIndex < 0 {
		dbIndex, err = d.getDbIndex(true, env)
	}
	return dbIndex, err
}

func (d *dbSelector) forceBackground(env string) (mctech.DbIndex, error) {
	// 从缓存中取取的当前正在用的库的索引
	indexFromRedis, err := d.getDbIndex(false, env)
	if err != nil {
		return -1, err
	}
	// 取后端库
	bgIndex := indexFromRedis ^ 0x0003
	return bgIndex, nil
}

func (d *dbSelector) getDbIndexByTicket(env string, requestId string) (mctech.DbIndex, error) {
	// 从缓存中获取，如果不存在就创建一个
	if value, ok := d.ticketMap.Get(requestId); ok {
		return value.(mctech.DbIndex), nil
	}

	value, err := d.getDbIndexByTicketFromService(env, requestId)
	if err != nil {
		return -1, err
	}
	d.ticketMap.Set(requestId, value, 0)
	return value, nil
}

// 从缓存中取取的当前正在用的库的索引
func (d *dbSelector) getDbIndex(local bool, env string) (mctech.DbIndex, error) {
	if local {
		if value, ok := d.currentMap.Get(localCacheKey); ok {
			return value.(mctech.DbIndex), nil
		}
	}

	// 本地缓存不存在，从远程服务中获取
	index, err := d.getDbIndexFromService(env)
	if err != nil {
		return -1, err
	}

	d.currentMap.Set(localCacheKey, index, 0)
	return index, nil
}

func (d *dbSelector) getDbIndexFromService(env string) (mctech.DbIndex, error) {
	option := mctech.GetOption()
	apiPrefix := option.DbChecker_ApiPrefix
	apiUrl := fmt.Sprintf("%scurrent-db?env=%s", apiPrefix, env)
	get, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return -1, err
	}

	body, err := mctech.DoRequest(get)
	if err != nil {
		return -1, err
	}

	var js = map[string]mctech.DbIndex{}
	err = json.Unmarshal(body, &js)
	if err != nil {
		return -1, errors.Wrap(err, "get dw index errors：" + apiPrefix)
	}
	return js["current"], nil
}

func (d *dbSelector) getDbIndexByTicketFromService(env string, requestId string) (mctech.DbIndex, error) {
	option := mctech.GetOption()
	apiPrefix := option.DbChecker_ApiPrefix
	apiUrl := fmt.Sprintf("%sdb;by-request?env=%s&request_id=%s", apiPrefix, env, requestId)
	get, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return -1, err
	}

	body, err := mctech.DoRequest(get)
	if err != nil {
		return -1, err
	}

	var js = map[string]mctech.DbIndex{}
	err = json.Unmarshal(body, &js)
	if err != nil {
		return -1, errors.Wrap(err, "get dw index by request errors")
	}

	return js["db"], nil
}
