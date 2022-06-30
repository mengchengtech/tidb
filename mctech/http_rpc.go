package mctech

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

var apiClient = &http.Client{
	Transport: &http.Transport{
		IdleConnTimeout: time.Second,
		// DisableKeepAlives: true,
	},
}

func DoRequest(request *http.Request) ([]byte, error) {
	var err error
	retryCount := 3
	for retryCount > 0 {
		body, err := do(request)
		if err == nil {
			return body, nil
		}
		retryCount--
	}

	return nil, err
}

func do(request *http.Request) ([]byte, error) {
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
