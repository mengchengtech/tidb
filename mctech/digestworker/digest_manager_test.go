package digestworker_test

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/digestworker"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestReloadDenyDigests(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"SQLChecker.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
	}()
	store := testkit.CreateMockStore(t)
	dom, _ := session.GetDomain(store)
	m := digestworker.NewDigestManager(nil)
	dom.SetDenyDigestManagerForTest(m)
	tk := testkit.NewTestKit(t, store)
	initDbAndData(tk)
	m.ReloadDenyDigests(tk.Session())

	info1 := m.Get("digest-1")
	require.Nil(t, info1)
	info2 := m.Get("digest-2")
	require.NotNil(t, info2)
	require.Equal(t, info2.ExpiredAt(), time.Date(9999, 10, 1, 0, 0, 0, 0, time.Local))
}

func initDbAndData(tk *testkit.TestKit) {
	createSQL := `CREATE TABLE IF NOT EXISTS mysql.mctech_deny_digest (
		digest varchar(64) PRIMARY KEY,
		created_at datetime not null,
		expired_at datetime,
		last_request_time datetime NULL,
    query_sql longtext not null,
		remark text
	);`
	tk.MustExec(createSQL)
	tk.MustExec(`insert into mysql.mctech_deny_digest
	(digest, created_at, expired_at, last_request_time, query_sql)
	values
	('digest-1', '2024-05-01', '2024-06-01', null, 'select 1')
	, ('digest-2', '2024-05-01', '9999-10-01', null, 'select 2')
	`)
}
