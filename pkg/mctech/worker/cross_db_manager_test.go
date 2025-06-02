package worker_test

import (
	"testing"

	mctechworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestReloadDBCross(t *testing.T) {
	store := testkit.CreateMockStore(t)
	dom, _ := session.GetDomain(store)
	m := mctechworker.NewCrossDBManager(nil)
	dom.SetServiceCrossDBManagerForTest(m)
	tk := testkit.NewTestKit(t, store)
	initMCTechCrossDB(tk)
	err := m.ReloadAll(tk.Session())
	require.NoError(t, err)

	info1 := m.Get("service-1")
	require.Equal(t,
		&mctechworker.ServiceCrossDBInfo{"service-1", false, []mctechworker.CrossDBGroup{{ID: 1, DBList: []string{"global_cq3", "global_ec5"}}}},
		info1)
	info2 := m.Get("service-2")
	require.NotNil(t, info2)
	require.Equal(t,
		&mctechworker.ServiceCrossDBInfo{"service-2", false, []mctechworker.CrossDBGroup{
			{ID: 2, DBList: []string{"global_cq2", "global_ec5", "global_mp"}},
			{ID: 3, DBList: []string{"global_qa", "global_ec3"}},
		}},
		info2)
	info50 := m.Get("service-allow-all")
	require.Equal(t,
		&mctechworker.ServiceCrossDBInfo{"service-allow-all", true, []mctechworker.CrossDBGroup{{ID: 50, DBList: []string{}}}},
		info50)

	info4 := m.Get("service-empty")
	require.Nil(t, info4)
	info100 := m.Get("service-disable")
	require.Nil(t, info100)
	info5 := m.Get("service-one-db")
	require.Nil(t, info5)
	infoNo := m.Get("service-no-data")
	require.Nil(t, infoNo)

	rs := tk.MustQuery("select id, loading_result from mysql.mctech_service_cross_dbs order by id")
	rs.Check([][]any{
		{"1", "Success."},
		{"2", "Success."},
		{"3", "Success."},
		{"4", "Ignore. The number of databases is less than 2"},
		{"5", "Ignore. The number of databases is less than 2"},
		{"50", "Success."},
		{"100", nil},
	})
}

func initMCTechCrossDB(tk *testkit.TestKit) {
	tk.MustExec(mctechworker.CreateMCTechServiceCrossDB)
	tk.MustExec(`insert into mysql.mctech_service_cross_dbs
	(id, service, cross_all_dbs, cross_dbs, enabled, created_at)
	values
	(1, 'service-1', false, 'global_cq3,global_ec5', true, '2024-05-01')
	, (2, 'service-2', false, 'global_cq2,global_ec5,global_mp',true, '2024-05-01')
	, (3, 'service-2', false, 'global_qa,global_ec3', true, '2024-05-01')
	, (4, 'service-empty', false, '', true, '2024-05-01')
	, (5, 'service-one-db', false, 'global_qa', true, '2024-05-01')
	, (50, 'service-allow-all', true, '', true, '2024-05-01')
	, (100, 'service-disable', false, 'global_qa, global_sq', false, '2024-05-01')
	`)
}
