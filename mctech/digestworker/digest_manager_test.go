package digestworker_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/mctech/digestworker"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestReloadDenyDigests(t *testing.T) {
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
    query longtext,
		expired_at datetime not null,
		last_request_time datetime NULL
	);`
	tk.MustExec(createSQL)
	tk.MustExec(`insert into mysql.mctech_deny_digest
	(digest, query, expired_at, last_request_time)
	values
	('digest-1', null, '2024-06-01', null)
	, ('digest-2', null, '9999-10-01', null)
	`)
}
