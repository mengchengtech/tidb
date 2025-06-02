package worker_test

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	mctechworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestReloadDenyDigests(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"SQLChecker.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()
	store := testkit.CreateMockStore(t)
	dom, _ := session.GetDomain(store)
	m := mctechworker.NewDigestManager(nil)
	dom.SetDenyDigestManagerForTest(m)
	tk := testkit.NewTestKit(t, store)
	initMCTechDenyDigest(tk)
	err := m.ReloadAll(tk.Session())
	require.NoError(t, err)

	info1 := m.Get("digest-1")
	require.Nil(t, info1)
	info2 := m.Get("digest-2")
	require.NotNil(t, info2)
	require.Equal(t, info2.ExpiredAt, time.Date(9999, 10, 1, 0, 0, 0, 0, time.Local))
}

func initMCTechDenyDigest(tk *testkit.TestKit) {
	tk.MustExec(mctechworker.CreateMCTechDenyDigest)
	tk.MustExec(`insert into mysql.mctech_deny_digest
	(digest, created_at, expired_at, last_request_time, query_sql)
	values
	('digest-1', '2024-05-01', '2024-06-01', null, 'select 1')
	, ('digest-2', '2024-05-01', '9999-10-01', null, 'select 2')
	`)
}
