package worker_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/mctech/mock"
	mcworker "github.com/pingcap/tidb/mctech/worker"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func createPattern(name string, tp mcworker.InvokerType) mcworker.SQLInvokerPattern {
	return mcworker.NewSQLInvokerPattern(name, tp)
}

type crossDBManagerCase struct {
	ID      int64
	State   mcworker.ResultStateType
	Message string
	Detail  *mcworker.CrossDBDetailData
}

func runCrossDBManagerCase(t *testing.T, m domain.CrossDBManager, tp mcworker.InvokerType) {
	info1 := m.Get(createPattern("invoker-1", tp))
	require.Equal(t, mcworker.NewCrossDBInfo(false, nil, []mcworker.CrossDBGroup{
		{ID: 1001, DBList: []string{"global_cq3", "global_ec5"}},
	}), info1)
	info2 := m.Get(createPattern("invoker-2", tp))
	require.NotNil(t, info2)
	require.Equal(t, mcworker.NewCrossDBInfo(false, nil, []mcworker.CrossDBGroup{
		{ID: 1002, DBList: []string{"global_cq2", "global_ec5", "global_mp"}},
		{ID: 1003, DBList: []string{"global_qa", "global_ec3"}},
	}), info2)
	infoExclude := m.Get(createPattern("invoker-exclude", tp))
	require.Empty(t, infoExclude.Exclude([]string{"global_bc", "global_ds"}))
	require.Equal(t, []string{"global_ipm", "global_cq3"}, infoExclude.Exclude([]string{"global_bc", "global_ipm", "global_cq3"}))
	info50 := m.Get(createPattern("invoker-allow-all", tp))
	require.Equal(t, mcworker.NewCrossDBInfo(true, nil, nil), info50)
	infoAny := m.Get(createPattern(mcworker.MatchAnyInvoker, tp))
	require.Equal(t, mcworker.NewCrossDBInfo(false, nil, []mcworker.CrossDBGroup{
		// internal init global rule
		{ID: 1, DBList: []string{"global_mtlp", "global_ma"}},
		// custom rule
		{ID: 1006, DBList: []string{"global_qa", "global_mp"}},
	}), infoAny)

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
	failpoint.Enable("github.com/pingcap/tidb/session/mctech-ddl-upgrade", mock.M(t, "false"))
	defer failpoint.Disable("github.com/pingcap/tidb/session/mctech-ddl-upgrade")

	invokerTypes := []mcworker.InvokerType{
		mcworker.InvokerTypeService,
		mcworker.InvokerTypePackage,
		mcworker.InvokerTypeBoth,
	}
	for _, tp := range invokerTypes {
		typeName := mcworker.AllInvokerTypes[tp]
		func() {
			session.RegisterMCTechUpgradeForTest(typeName, func(ctx context.Context, s session.Session) error {
				return initMCTechCrossDB(ctx, s, tp)
			})
			defer session.UnregisterMCTechUpgradeForTest(typeName)

			dotestReloadCrossDBManagerByType(t, tp)
		}()
	}
}

func dotestReloadCrossDBManagerByType(t *testing.T, tp mcworker.InvokerType) {
	now := time.Now()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	mgr, _ := dom.CrossDBManager()

	require.Empty(t, mgr.Exclude([]string{"global_ipm", "global_dw_1", "global_dw_2", "global_dwb"}))
	require.Equal(t, []string{"global_ec3"}, mgr.Exclude([]string{"global_ec3", "global_platform"}))

	switch tp {
	case mcworker.InvokerTypeService, mcworker.InvokerTypePackage:
		runCrossDBManagerCase(t, mgr, tp)
	case mcworker.InvokerTypeBoth:
		runCrossDBManagerCase(t, mgr, mcworker.InvokerTypeService)
		runCrossDBManagerCase(t, mgr, mcworker.InvokerTypePackage)
	}

	cases := []crossDBManagerCase{
		{1, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail([][]string{{"global_mtlp", "global_ma"}}, nil, "*", mcworker.InvokerTypeBoth, false)},
		{2, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail(nil, &mcworker.FilterData{Global: true, Patterns: []string{"global_ipm", "global_platform"}}, "*", mcworker.InvokerTypeBoth, false)},
		{3, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail(nil, &mcworker.FilterData{Global: true, Patterns: []string{"global_dw_*", "global_dwb"}}, "*", mcworker.InvokerTypeBoth, false)},
		{4, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail(nil, nil, "@mctech/dp-impala-tidb-enhanced", mcworker.InvokerTypePackage, true)},
		{1001, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail([][]string{{"global_cq3", "global_ec5"}}, nil, "invoker-1", tp, false)},
		{1002, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail([][]string{{"global_cq2", "global_ec5", "global_mp"}}, nil, "invoker-2", tp, false)},
		{1003, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail([][]string{{"global_qa", "global_ec3"}}, nil, "invoker-2", tp, false)},
		{1004, mcworker.ResultStateTypeError, "Ignore. The 'cross_dbs' field is empty.", nil},
		{1005, mcworker.ResultStateTypeError, "Ignore. The number of databases in group(0) is less than 2.", nil},
		{1006, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail([][]string{{"global_qa", "global_mp"}}, nil, mcworker.MatchAnyInvoker, tp, false)},
		{1007, mcworker.ResultStateTypeError, "Ignore. The 'allow_all_dbs' field should not be false, when invoker_name is '*'.", nil},
		{1008, mcworker.ResultStateTypeError, "Ignore. The 'invoker_name' field is empty.", nil},
		{1009, mcworker.ResultStateTypeError, "Ignore. The 'invoker_name' field is empty.", nil},
		{1010, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail(nil, &mcworker.FilterData{Global: false, Patterns: []string{"global_bc", "global_ds"}}, "invoker-exclude", tp, false)},
		{1050, mcworker.ResultStateTypeSuccess, "Loaded Success", mcworker.CreateCrossDBDetail(nil, nil, "invoker-allow-all", tp, true)},
		{1100, mcworker.ResultStateTypeDisabled, "current rule is Disabled", nil},
		{1101, mcworker.ResultStateTypeDisabled, "current rule is Disabled", nil},
	}

	for i, actual := range mgr.GetLoadedResults() {
		expected := cases[i]
		message := fmt.Sprintf("%v", cases[i])
		require.Equal(t, expected.ID, actual.ID, message)
		require.Equal(t, expected.State, actual.Data.State, message)
		require.Equal(t, expected.Message, actual.Data.Message, message)
		require.GreaterOrEqual(t, actual.Data.LoadedAt, now, message)

		if expected.Detail == nil {
			continue
		}
		require.Equal(t, expected.Detail.Service, actual.Data.Detail.Service, message)
		require.Equal(t, expected.Detail.Package, actual.Data.Detail.Package, message)
		require.Equal(t, expected.Detail.AllowAllDBs, actual.Data.Detail.AllowAllDBs, message)
		require.Equal(t, expected.Detail.CrossDBGroups, actual.Data.Detail.CrossDBGroups, message)
		require.Equal(t, expected.Detail.Filters, actual.Data.Detail.Filters, message)
	}
}

func initMCTechCrossDB(ctx context.Context, sctx session.Session, tp mcworker.InvokerType) (err error) {
	ctx = util.WithInternalSourceType(ctx, "initMCTechCrossDB")
	args := []any{
		mysql.SystemDB, mcworker.MCTechCrossDB,
	}
	sctx.ExecuteInternal(ctx, "delete from %n.%n where id > 1000", args...)
	sctx.ExecuteInternal(ctx, strings.ReplaceAll(`insert into %n.%n
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
	, (1010, 'invoker-exclude', '{type}', false, 'global_ds,global_bc,*', true, '2024-05-01')
	, (1050, 'invoker-allow-all', '{type}', true, '', true, '2024-05-01')
	, (1100, 'invoker-disable', '{type}', false, 'global_qa, global_sq', false, '2024-05-01')
	, (1101, '', '{type}', true, '', false, '2024-05-01')
	`, "{type}", mcworker.AllInvokerTypes[tp]),
		args...)
	return err
}
