package preps_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx"
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
	Source(i int) any
	Failure() string
}

type mctechSessionTestCase interface {
	mctechTestCase
	Roles() []string
}

func initSession(s sessionctx.Context, roles []string) {
	vars := s.GetSessionVars()
	vars.User = &auth.UserIdentity{Username: "mock_user", Hostname: "%"}

	if len(roles) > 0 {
		ar := make([]*auth.RoleIdentity, len(roles))
		for i, r := range roles {
			ar[i] = &auth.RoleIdentity{Username: r, Hostname: "%"}
		}
		vars.ActiveRoles = ar
	}
}

type runTestCaseWithSessionType[T mctechSessionTestCase] func(t *testing.T, i int, c T, mctechCtx mctech.Context) error
type runTestCaseType[T mctechTestCase] func(t *testing.T, i int, c T) error

type parser interface {
	Parse(ctx context.Context, sql string) ([]ast.StmtNode, error)
}

var dbMap = map[string]string{
	"pf": "global_platform",
	"pd": "public_data",
	"ac": "asset_component",
}

func doRunTest[T mctechTestCase](t *testing.T, runTestCase runTestCaseType[T], cases []T) {
	for i, c := range cases {
		err := runTestCase(t, i, c)
		failure := c.Failure()
		if err == nil && failure == "" {
			continue
		}

		if failure != "" {
			require.ErrorContainsf(t, err, failure, "source %v", c.Source(i))
		} else {
			require.NoErrorf(t, err, "source %v", c.Source(i))
		}
	}
}

func doRunWithSessionTest[T mctechSessionTestCase](t *testing.T, runTestCase runTestCaseWithSessionType[T], cases []T) {
	store := testkit.CreateMockStore(t)
	tk := initMock(t, store)

	session := tk.Session()

	for i, c := range cases {
		initSession(session, c.Roles())
		mctechCtx, err := mctech.WithNewContext(session)
		require.NoError(t, err)

		err = runTestCase(t, i, c, mctechCtx)
		if err == nil {
			continue
		}

		if c.Failure() != "" {
			require.ErrorContainsf(t, err, c.Failure(), "source %v", c.Source(i))
		} else {
			require.NoErrorf(t, err, "source %v", c.Source(i))
		}
	}
}
