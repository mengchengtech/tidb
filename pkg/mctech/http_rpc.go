package mctech

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var apiClient = &http.Client{
	Transport: &http.Transport{
		IdleConnTimeout: time.Second,
		// DisableKeepAlives: true,
	},
}

// DoRequest invoke rpc api
func DoRequest(request *http.Request) (body []byte, statusCode int, err error) {
	retryCount := 3
	for retryCount > 0 {
		body, statusCode, err = do(request)
		if err == nil {
			return body, statusCode, nil
		}
		retryCount--
	}

	if err != nil {
		log.Error("rpc调用发生错误。",
			zap.String("method", request.Method),
			zap.String("uri", request.RequestURI),
			zap.Error(err),
			zap.Stack("stack"),
		)
	}
	return nil, statusCode, errors.New("rpc调用发生错误。详情请查询tidb服务日志")
}

func do(request *http.Request) ([]byte, int, error) {
	failpoint.Inject("MockMctechHttp", func(val failpoint.Value) {
		values := make(map[string]any)
		var err error
		if val != nil {
			err = json.Unmarshal([]byte(val.(string)), &values)
			if err != nil {
				panic(err)
			}
		}
		path := request.URL.Path
		var (
			res        any
			ok         bool
			statusCode = 200
		)
		switch {
		case strings.HasSuffix(path, "/db/aes"):
			res, ok = values["Crypto.AES"]
		case strings.HasSuffix(path, "/crypto"):
			// 使用旧的获取方式 '/db/aes'
			if old, ok := values["Crypto.OLD"]; ok && old.(bool) {
				failpoint.Return(nil, 404, err)
			}
			res, ok = values["Crypto.CRYPTO"]
		case strings.HasSuffix(path, "/version"):
			res, ok = values["Sequence.Version"]
		case strings.HasSuffix(path, "/nexts"):
			res, ok = values["Sequence.Nexts"]
		case strings.HasSuffix(path, "/current-db"):
			res, ok = values["DWIndex.Current"]
		case strings.HasPrefix(path, "/db;by-request"):
			res, ok = values["DWIndex.ByRequest"]
		}

		if ok {
			var data []byte
			switch x := res.(type) {
			case string:
				data = []byte(x)
			default:
				data, err = json.Marshal(x)
				if err != nil {
					panic(err)
				}
			}
			failpoint.Return(data, statusCode, err)
		}

		if !strings.HasPrefix(path, "/rpc-test") {
			failpoint.Return(nil, statusCode, err)
		}
	})

	response, err := apiClient.Do(request)
	if err != nil {
		// 网络问题或者是服务器不定时出的502错误，重试几次
		var statusCode = 0
		if response != nil {
			statusCode = response.StatusCode
		}
		return nil, statusCode, err
	}

	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, response.StatusCode, err
	}

	if response.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("%d: %s >> %s", response.StatusCode, response.Status, string(body))
	}

	if err != nil {
		return nil, response.StatusCode, err
	}
	return body, response.StatusCode, err
}
