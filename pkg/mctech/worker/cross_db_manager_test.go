package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	mcworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func createPattern(name string, tp mcworker.InvokerType) mcworker.SQLInvokerPattern {
	return mcworker.NewSQLInvokerPattern(name, tp)
}

type crossDBManagerCase struct {
	ID      int64
	State   string
	Message string
	Data    *mcworker.CrossDBData
}

func createCrossDBData(crossDBs []string, name string, tp mcworker.InvokerType, allowAllDBs bool) *mcworker.CrossDBData {
	data := &mcworker.CrossDBData{CrossDBs: crossDBs}
	if allowAllDBs {
		data.AllowAllDBs = &allowAllDBs
	}
	switch tp {
	case mcworker.InvokerTypeService:
		data.Service = name
	case mcworker.InvokerTypePackage:
		data.Package = name
	case mcworker.InvokerTypeBoth:
		data.Service = name
		data.Package = name
	}
	return data
}

func runCrossDBManagerCase(t *testing.T, m *mcworker.CrossDBManager, tp mcworker.InvokerType) {
	info1 := m.Get(createPattern("invoker-1", tp))
	require.Equal(t, &mcworker.CrossDBInfo{false, []mcworker.CrossDBGroup{
		{ID: 1001, DBList: []string{"global_cq3", "global_ec5"}},
	}}, info1)
	info2 := m.Get(createPattern("invoker-2", tp))
	require.NotNil(t, info2)
	require.Equal(t, &mcworker.CrossDBInfo{false, []mcworker.CrossDBGroup{
		{ID: 1002, DBList: []string{"global_cq2", "global_ec5", "global_mp"}},
		{ID: 1003, DBList: []string{"global_qa", "global_ec3"}},
	}}, info2)
	info50 := m.Get(createPattern("invoker-allow-all", tp))
	require.Equal(t, &mcworker.CrossDBInfo{true, nil}, info50)
	infoAny := m.Get(createPattern(mcworker.MatchAnyInvoker, tp))
	require.Equal(t, &mcworker.CrossDBInfo{true, []mcworker.CrossDBGroup{
		{ID: 1006, DBList: []string{"global_qa", "global_mp"}},
	}}, infoAny)

	info4 := m.Get(createPattern("invoker-empty", tp))
	require.Nil(t, info4)
	info100 := m.Get(createPattern("invoker-disable", tp))
	require.Nil(t, info100)
	info5 := m.Get(createPattern("invoker-one-db", tp))
	require.Nil(t, info5)
	infoNo := m.Get(createPattern("invoker-no-data", tp))
	require.Nil(t, infoNo)
}

func TestReloadCrossDBManager(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade", mock.M(t, "false"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	for _, tp := range []mcworker.InvokerType{
		mcworker.InvokerTypeService, mcworker.InvokerTypePackage, mcworker.InvokerTypeBoth} {
		initMCTechCrossDB(tk, tp)
		m := mcworker.NewCrossDBManager(nil)
		err := m.ReloadAll(tk.Session())
		require.NoError(t, err)

		switch tp {
		case mcworker.InvokerTypeService:
		case mcworker.InvokerTypePackage:
			runCrossDBManagerCase(t, m, tp)
		case mcworker.InvokerTypeBoth:
			runCrossDBManagerCase(t, m, mcworker.InvokerTypeService)
			runCrossDBManagerCase(t, m, mcworker.InvokerTypePackage)
		}

		rs := tk.MustQuery(fmt.Sprintf("select id, loaded_result from %s.%s order by id", mysql.SystemDB, mcworker.MCTechCrossDB))
		cases := []crossDBManagerCase{
			{1001, "success", "Loaded Success", createCrossDBData([]string{"global_cq3", "global_ec5"}, "invoker-1", tp, false)},
			{1002, "success", "Loaded Success", createCrossDBData([]string{"global_cq2", "global_ec5", "global_mp"}, "invoker-2", tp, false)},
			{1003, "success", "Loaded Success", createCrossDBData([]string{"global_qa", "global_ec3"}, "invoker-2", tp, false)},
			{1004, "error", "Ignore. The number of databases is less than 2", nil},
			{1005, "error", "Ignore. The number of databases is less than 2", nil},
			{1006, "success", "Loaded Success", createCrossDBData([]string{"global_qa", "global_mp"}, mcworker.MatchAnyInvoker, tp, false)},
			{1007, "success", "Loaded Success", createCrossDBData(nil, mcworker.MatchAnyInvoker, tp, true)},
			{1008, "error", "Ignore. The invoker_name field must not be empty.", nil},
			{1009, "error", "Ignore. The invoker_name field must not be empty.", nil},
			{1050, "success", "Loaded Success", createCrossDBData(nil, "invoker-allow-all", tp, true)},
			{1100, "disabled", "current rule is Disabled", nil},
			{1101, "disabled", "current rule is Disabled", nil},
		}

		var cells [][]any
		for _, c := range cases {
			row := []any{strconv.FormatInt(c.ID, 10), c}
			cells = append(cells, row)
		}
		rs.CheckWithFunc(cells, func(actual []string, expected []any) bool {
			require.Equal(t, len(expected), len(actual))
			require.Equal(t, expected[0], actual[0]) // ID
			c := expected[1].(crossDBManagerCase)
			var result []any
			if err := json.Unmarshal([]byte(actual[1]), &result); err != nil {
				panic(err)
			}
			require.Equal(t, c.State, result[0])   // State
			require.Equal(t, c.Message, result[1]) // Message
			if len(result) == 2 {
				return true
			}

			bytes, _ := json.Marshal(result[2])
			var dd mcworker.CrossDBData
			if err := json.Unmarshal(bytes, &dd); err != nil {
				panic(err)
			}
			require.Equal(t, c.Data.Service, dd.Service)
			require.Equal(t, c.Data.Package, dd.Package)
			require.Equal(t, c.Data.AllowAllDBs, dd.AllowAllDBs)
			require.Equal(t, c.Data.CrossDBs, dd.CrossDBs)

			return true
		})
	}
}

func initMCTechCrossDB(tk *testkit.TestKit, tp mcworker.InvokerType) {
	ctx := util.WithInternalSourceType(context.Background(), "initMCTechCrossDB")
	args := []any{
		mysql.SystemDB, mcworker.MCTechCrossDB,
	}
	tk.Session().ExecuteInternal(ctx, "truncate table %n.%n", args...)
	tk.Session().ExecuteInternal(ctx, strings.ReplaceAll(`insert into %n.%n
	(id, invoker_name, invoker_type, allow_all_dbs, cross_dbs, enabled, created_at)
	values
	(1001, 'invoker-1', '{type}', false, 'global_cq3,global_ec5', true, '2024-05-01')
	, (1002, 'invoker-2', '{type}', false, 'global_cq2,global_ec5,global_mp',true, '2024-05-01')
	, (1003, 'invoker-2', '{type}', false, 'global_qa,global_ec3', true, '2024-05-01')
	, (1004, 'invoker-empty', '{type}', false, '', true, '2024-05-01')
	, (1005, 'invoker-one-db', '{type}', false, 'global_qa', true, '2024-05-01')
	, (1006, '*', '{type}', false, 'global_qa,global_mp', true, '2024-05-01')
	, (1007, '*', '{type}', true, '', true, '2024-05-01')
	, (1008, '', '{type}', true, '', true, '2024-05-01')
	, (1009, '', '{type}', false, '', true, '2024-05-01')
	, (1050, 'invoker-allow-all', '{type}', true, '', true, '2024-05-01')
	, (1100, 'invoker-disable', '{type}', false, 'global_qa, global_sq', false, '2024-05-01')
	, (1101, '', '{type}', true, '', false, '2024-05-01')
	`, "{type}", string(tp)),
		args...)
}
