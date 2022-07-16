package prapared

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/auth"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_platform")
	tk.MustExec("create database global_platform")
	tk.MustExec("use global_platform")
	tk.MustExec("create table t(a int, b int, key(b))")

	return tk
}

type mctechTestCase interface {
	Source() any
	Failure() string
}

func createSession(t *testing.T, tk *testkit.TestKit, user string, roles ...string) session.Session {
	session := tk.Session()
	vars := session.GetSessionVars()
	vars.User = &auth.UserIdentity{Username: user, Hostname: "%"}

	if len(roles) > 0 {
		ar := make([]*auth.RoleIdentity, len(roles))
		for i, r := range roles {
			ar[i] = &auth.RoleIdentity{Username: r, Hostname: "%"}
		}
		vars.ActiveRoles = ar
	}

	factory := GetHandlerFactory()
	mctech.SetHandlerFactory(session, factory)
	return session
}

type runTestCaseWithSessionType[T mctechTestCase] func(t *testing.T, c T, session session.Session) error
type runTestCaseType[T mctechTestCase] func(t *testing.T, c T) error

var dbMap = map[string]string{
	"pf": "global_platform",
	"pd": "public_data",
	"ac": "asset_component",
}

func doRunTest[T mctechTestCase](t *testing.T, runTestCase runTestCaseType[T], cases []T) {
	for _, c := range cases {
		err := runTestCase(t, c)
		failure := c.Failure()
		if err == nil && failure == "" {
			continue
		}

		if failure != "" {
			require.ErrorContainsf(t, err, failure, "source %v", c.Source())
		} else {
			require.NoErrorf(t, err, "source %v", c.Source())
		}
	}
}

func doRunWithSessionTest[T mctechTestCase](t *testing.T, runTestCase runTestCaseWithSessionType[T], cases []T, user string, roles ...string) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)
	session := createSession(t, tk, user, roles...)

	for _, c := range cases {
		err := runTestCase(t, c, session)
		if err == nil {
			continue
		}

		if c.Failure() != "" {
			require.ErrorContains(t, err, c.Failure())
		} else {
			require.NoErrorf(t, err, "source %v", c.Source())
		}
	}
}

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
