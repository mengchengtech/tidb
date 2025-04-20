package preps

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
	"github.com/pkg/errors"
)

const paramBackgroundKey = "background"
const paramRequestIDKey = "requestId"

// 整体切换前后台库的参数值KEY
const localCacheKey = "$$global"

var ticketMap = NewCache(60*time.Second, 20*time.Second)
var currentMap = NewCache(15*time.Second, 10*time.Second)

type dwSelector struct {
	// private final static URI BASE_URI;
	result  mctech.PrepareResult
	dwIndex *mctech.DWIndex
}

func newDWSelector(result mctech.PrepareResult) mctech.DWSelector {
	return &dwSelector{
		result: result,
	}
}

func (d *dwSelector) GetDWIndex() (mctech.DWIndex, error) {
	if d.dwIndex != nil {
		return *d.dwIndex, nil
	}

	result := d.result
	params := result.Params()
	env := result.DbPrefix()
	var dwIndex = mctech.DWIndexNone
	var err error
	_, forceBackground := params[paramBackgroundKey]
	if forceBackground {
		// 强制使用后台库
		dwIndex, err = d.forceBackground(env)
	} else {
		if ticket, ok := params[paramRequestIDKey]; ok {
			// 同一个ticket使用相同的后端库
			dwIndex, err = d.getDWIndexByTicket(env, ticket.(string))
		}
	}

	if err != nil {
		return mctech.DWIndexNone, err
	}

	if dwIndex < 0 {
		dwIndex, err = d.getDWIndex(true, env)
	}

	if dwIndex > 0 {
		d.dwIndex = new(mctech.DWIndex)
		*d.dwIndex = dwIndex
	}
	return dwIndex, err
}

func (d *dwSelector) forceBackground(env string) (mctech.DWIndex, error) {
	// 从缓存中取取的当前正在用的库的索引
	indexFromRedis, err := d.getDWIndex(false, env)
	if err != nil {
		return mctech.DWIndexNone, err
	}
	// 取后端库
	bgIndex := indexFromRedis ^ 0x0003
	return bgIndex, nil
}

func (d *dwSelector) getDWIndexByTicket(env string, requestID string) (mctech.DWIndex, error) {
	// 从缓存中获取，如果不存在就创建一个
	if value, ok := ticketMap.Get(requestID); ok {
		return value.(mctech.DWIndex), nil
	}

	value, err := d.getDWIndexByTicketFromService(env, requestID)
	if err != nil {
		return mctech.DWIndexNone, err
	}
	ticketMap.Set(requestID, value)
	return value, nil
}

// 从缓存中取取的当前正在用的库的索引
func (d *dwSelector) getDWIndex(local bool, env string) (mctech.DWIndex, error) {
	if local {
		if value, ok := currentMap.Get(localCacheKey); ok {
			return value.(mctech.DWIndex), nil
		}
	}

	// 本地缓存不存在，从远程服务中获取
	index, err := d.getDWIndexFromService(env)
	if err != nil {
		return mctech.DWIndexNone, err
	}

	currentMap.Set(localCacheKey, index)
	return index, nil
}

func (d *dwSelector) getDWIndexFromService(env string) (mctech.DWIndex, error) {
	apiPrefix := config.GetMCTechConfig().DbChecker.APIPrefix
	apiURL := fmt.Sprintf("%scurrent-db?env=%s", apiPrefix, env)
	get, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return mctech.DWIndexNone, err
	}

	body, err := mctech.DoRequest(get)
	if err != nil {
		return mctech.DWIndexNone, err
	}

	var js = map[string]mctech.DWIndex{}
	err = json.Unmarshal(body, &js)
	if err != nil {
		return mctech.DWIndexNone, errors.Wrap(err, "get dw index errors")
	}
	return js["current"], nil
}

func (d *dwSelector) getDWIndexByTicketFromService(env string, requestID string) (mctech.DWIndex, error) {
	apiPrefix := config.GetMCTechConfig().DbChecker.APIPrefix
	apiURL := fmt.Sprintf("%sdb;by-request?env=%s&request_id=%s", apiPrefix, env, requestID)
	get, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return mctech.DWIndexNone, err
	}

	body, err := mctech.DoRequest(get)
	if err != nil {
		return mctech.DWIndexNone, err
	}

	var js = map[string]mctech.DWIndex{}
	err = json.Unmarshal(body, &js)
	if err != nil {
		return mctech.DWIndexNone, errors.Wrap(err, "get dw index by request errors")
	}

	return js["db"], nil
}
