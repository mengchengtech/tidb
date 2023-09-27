package preps

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/tidb/mctech"
	"github.com/pkg/errors"
)

const paramBackgroundKey = "background"
const paramRequestIDKey = "requestId"

// 整体切换前后台库的参数值KEY
const localCacheKey = "$$global"

var ticketMap = NewCache(60*time.Second, 20*time.Second)
var currentMap = NewCache(15*time.Second, 10*time.Second)

type dbSelector struct {
	// private final static URI BASE_URI;
	result  *mctech.PrepareResult
	dbIndex *mctech.DbIndex
}

func newDBSelector(result *mctech.PrepareResult) mctech.DBSelector {
	return &dbSelector{
		result: result,
	}
}

func (d *dbSelector) GetDbIndex() (mctech.DbIndex, error) {
	if d.dbIndex != nil {
		return *d.dbIndex, nil
	}

	result := d.result
	params := result.Params()
	env := result.DbPrefix()
	var dbIndex mctech.DbIndex = -1
	var err error
	_, forceBackground := params[paramBackgroundKey]
	if forceBackground {
		// 强制使用后台库
		dbIndex, err = d.forceBackground(env)
	} else {
		if ticket, ok := params[paramRequestIDKey]; ok {
			// 同一个ticket使用相同的后端库
			dbIndex, err = d.getDbIndexByTicket(env, ticket.(string))
		}
	}

	if err != nil {
		return -1, err
	}

	if dbIndex < 0 {
		dbIndex, err = d.getDbIndex(true, env)
	}

	if dbIndex > 0 {
		d.dbIndex = new(mctech.DbIndex)
		*d.dbIndex = dbIndex
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

func (d *dbSelector) getDbIndexByTicket(env string, requestID string) (mctech.DbIndex, error) {
	// 从缓存中获取，如果不存在就创建一个
	if value, ok := ticketMap.Get(requestID); ok {
		return value.(mctech.DbIndex), nil
	}

	value, err := d.getDbIndexByTicketFromService(env, requestID)
	if err != nil {
		return -1, err
	}
	ticketMap.Set(requestID, value)
	return value, nil
}

// 从缓存中取取的当前正在用的库的索引
func (d *dbSelector) getDbIndex(local bool, env string) (mctech.DbIndex, error) {
	if local {
		if value, ok := currentMap.Get(localCacheKey); ok {
			return value.(mctech.DbIndex), nil
		}
	}

	// 本地缓存不存在，从远程服务中获取
	index, err := d.getDbIndexFromService(env)
	if err != nil {
		return -1, err
	}

	currentMap.Set(localCacheKey, index)
	return index, nil
}

func (d *dbSelector) getDbIndexFromService(env string) (mctech.DbIndex, error) {
	option := mctech.GetOption()
	apiPrefix := option.DbCheckerAPIPrefix
	apiURL := fmt.Sprintf("%scurrent-db?env=%s", apiPrefix, env)
	get, err := http.NewRequest("GET", apiURL, nil)
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
		return -1, errors.Wrap(err, "get dw index errors："+apiPrefix)
	}
	return js["current"], nil
}

func (d *dbSelector) getDbIndexByTicketFromService(env string, requestID string) (mctech.DbIndex, error) {
	option := mctech.GetOption()
	apiPrefix := option.DbCheckerAPIPrefix
	apiURL := fmt.Sprintf("%sdb;by-request?env=%s&request_id=%s", apiPrefix, env, requestID)
	get, err := http.NewRequest("GET", apiURL, nil)
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
