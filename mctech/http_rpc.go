package mctech

import (
	"errors"
	"fmt"
	"io"
	"net/http"
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
func DoRequest(request *http.Request) (body []byte, err error) {
	retryCount := 3
	for retryCount > 0 {
		body, err = do(request)
		if err == nil {
			return body, nil
		}
		retryCount--
	}

	return nil, err
}

func do(request *http.Request) ([]byte, error) {
	failpoint.Inject("MockMctechHttp", func(val failpoint.Value) {
		bytes := []byte(val.(string))
		failpoint.Return(bytes, nil)
	})

	response, err := apiClient.Do(request)
	if err != nil {
		// 网络问题或者是服务器不定时出的502错误，重试几次
		return nil, err
	}

	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("%d: %s >> %s", response.StatusCode, response.Status, string(body))
	}

	if err != nil {
		return nil, err
	}
	return body, err
}
