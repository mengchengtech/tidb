package preps

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
)

// 整体切换前后台库的参数值KEY
const localCacheKey = "$$global"

var ticketMap = NewCache(60*time.Second, 20*time.Second)
var currentMap = NewCache(15*time.Second, 10*time.Second)

type dwSelector struct {
	dwIndex *mctech.DWIndex
}

func newDWSelector() mctech.DWSelector {
	return &dwSelector{}
}

func (d *dwSelector) SelectIndex(dbPrefix, requestID string, forceBackground bool) (*mctech.DWIndex, error) {
	if d.dwIndex != nil {
		return d.dwIndex, nil
	}

	var dwIndex *mctech.DWIndex
	var err error
	if forceBackground {
		// 强制使用后台库
		dwIndex, err = d.forceBackground(dbPrefix)
	} else {
		if requestID != "" {
			// 同一个ticket使用相同的后端库
			dwIndex, err = d.getIndexByRequestID(dbPrefix, requestID)
		}
	}

	if err != nil {
		return nil, err
	}

	if dwIndex == nil {
		var info *mctech.DWIndexInfo
		if info, err = d.getIndexInfo(true, dbPrefix); err == nil {
			dwIndex = &info.Current
		}
	}

	d.dwIndex = dwIndex
	return dwIndex, err
}

func (d *dwSelector) GetIndexInfo(dbPrefix string) (*mctech.DWIndexInfo, error) {
	return d.getIndexInfo(true, dbPrefix)
}

func (d *dwSelector) forceBackground(dbPrefix string) (*mctech.DWIndex, error) {
	// 从缓存中取取的当前正在用的库的索引
	info, err := d.getIndexInfo(false, dbPrefix)
	if err != nil {
		return nil, err
	}
	// 取后端库
	return &info.Background, nil
}

func (d *dwSelector) getIndexByRequestID(dbPrefix string, requestID string) (*mctech.DWIndex, error) {
	// 从缓存中获取，如果不存在就创建一个
	if value, ok := ticketMap.Get(requestID); ok {
		index := value.(mctech.DWIndex)
		return &index, nil
	}

	index, err := d.getIndexByRequestIDFromService(dbPrefix, requestID)
	if err != nil {
		return nil, err
	}
	ticketMap.Set(requestID, *index)
	return index, nil
}

// 从缓存中取取的当前正在用的库的索引
func (d *dwSelector) getIndexInfo(local bool, dbPrefix string) (*mctech.DWIndexInfo, error) {
	if local {
		if value, ok := currentMap.Get(localCacheKey); ok {
			return value.(*mctech.DWIndexInfo), nil
		}
	}

	// 本地缓存不存在，从远程服务中获取
	info, err := d.getIndexFromService(dbPrefix)
	if err != nil {
		return nil, err
	}

	currentMap.Set(localCacheKey, info)
	return info, nil
}

func (d *dwSelector) getIndexFromService(dbPrefix string) (*mctech.DWIndexInfo, error) {
	apiPrefix := config.GetMCTechConfig().DbChecker.APIPrefix
	apiURL := fmt.Sprintf("%scurrent-db?env=%s", apiPrefix, dbPrefix)
	get, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}

	body, _, err := mctech.DoRequest(get)
	if err != nil {
		return nil, err
	}

	jo := map[string]mctech.DWIndex{}
	err = json.Unmarshal(body, &jo)
	if err != nil {
		return nil, fmt.Errorf("get dw index errors. %w", err)
	}

	info := &mctech.DWIndexInfo{Current: jo["current"]}

	if background, ok := jo["background"]; ok {
		info.Background = background
	} else {
		// 兼容旧的代码
		// TODO: rpc调用能返回background信息时，可以移除
		info.Background = info.Current ^ 0x0003
	}

	return info, nil
}

func (d *dwSelector) getIndexByRequestIDFromService(dbPrefix string, requestID string) (*mctech.DWIndex, error) {
	apiPrefix := config.GetMCTechConfig().DbChecker.APIPrefix
	apiURL := fmt.Sprintf("%sdb;by-request?env=%s&request_id=%s", apiPrefix, dbPrefix, requestID)
	get, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}

	body, _, err := mctech.DoRequest(get)
	if err != nil {
		return nil, err
	}

	var js = map[string]mctech.DWIndex{}
	err = json.Unmarshal(body, &js)
	if err != nil {
		return nil, fmt.Errorf("get dw index by request errors. %w", err)
	}

	if index, ok := js["db"]; ok {
		return &index, nil
	}
	return nil, fmt.Errorf("get db index error. dbPrefix: '%s', requestID: '%s'", dbPrefix, requestID)
}
