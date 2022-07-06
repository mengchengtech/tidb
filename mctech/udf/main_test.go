package udf

import (
	"bytes"
	"io/ioutil"
	"net/http"
)

type GetDoFuncType func(req *http.Request) (*http.Response, error)

var GetDoFunc GetDoFuncType

type MockClient struct {
}

func (m *MockClient) Do(req *http.Request) (*http.Response, error) {
	return GetDoFunc(req)
}

func createGetDoFunc(text string) GetDoFuncType {
	return func(req *http.Request) (*http.Response, error) {
		res := &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewReader([]byte(text))),
		}
		return res, nil
	}
}
