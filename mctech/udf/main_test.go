package udf

import (
	"bytes"
	"io/ioutil"
	"net/http"
)

type getDoFuncType func(req *http.Request) (*http.Response, error)

var getDoFunc getDoFuncType

type mockClient struct {
}

func (m *mockClient) Do(req *http.Request) (*http.Response, error) {
	return getDoFunc(req)
}

func createGetDoFunc(text string) getDoFuncType {
	return func(req *http.Request) (*http.Response, error) {
		res := &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewReader([]byte(text))),
		}
		return res, nil
	}
}
