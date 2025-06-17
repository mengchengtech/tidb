package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/mock"
	mcworker "github.com/pingcap/tidb/mctech/worker"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestReloadDenyDigests(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"SQLChecker.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
	}()
	store := testkit.CreateMockStore(t)
	m := mcworker.NewDigestManager(nil)
	tk := testkit.NewTestKit(t, store)
	initMCTechDenyDigest(tk)
	m.ReloadDenyDigests(tk.Session())

	info1 := m.Get("digest-1")
	require.Nil(t, info1)
	info2 := m.Get("digest-2")
	require.NotNil(t, info2)
	require.Equal(t, info2.ExpiredAt(), time.Date(9999, 10, 1, 0, 0, 0, 0, time.Local))
}

func initMCTechDenyDigest(tk *testkit.TestKit) {
	ctx := util.WithInternalSourceType(context.Background(), "initMCTechDenyDigest")
	args := []any{
		mysql.SystemDB, mcworker.MCTechDenyDigest,
	}
	tk.Session().ExecuteInternal(ctx, mcworker.CreateMCTechDenyDigest, args...)
	tk.Session().ExecuteInternal(ctx, `insert into %n.%n
	(digest, created_at, expired_at, last_request_time, query_sql)
	values
	('digest-1', '2024-05-01', '2024-06-01', null, 'select 1')
	, ('digest-2', '2024-05-01', '9999-10-01', null, 'select 2')
	`, args...)
}
