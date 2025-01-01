package interceptor_test

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/interceptor"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestFullSQLLogForExecuteNoParameterAndDisableTenant(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": false}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	rawSQL := "select * from project_construction_quantity_contract_bill_part"
	ctx := context.Background()
	// simple prepare and execute
	data := append([]byte{mysql.ComStmtPrepare}, []byte(rawSQL)...)
	require.NoError(t, cc.Dispatch(ctx, data))
	data = []byte{
		mysql.ComStmtExecute, // cmd
		0x01, 0x0, 0x0, 0x0,  // stmtID little endian
		0x00,               // useCursor == false
		0x1, 0x0, 0x0, 0x0, // iteration-count, always 1
	}
	require.NoError(t, cc.Dispatch(ctx, data))
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)
	sql := logData["sql"]
	require.Contains(t, sql, rawSQL+";Type=Binary;Params=[]")

	// -------------------------------------------------------------------------------------

	tk.MustExec("prepare stmt0 from '" + rawSQL + "'")
	tk.MustQuery("execute stmt0")
	logData, err = interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)
	sql = logData["sql"]
	require.Contains(t, sql, rawSQL+";Type=SQL;Name=stmt0;Params=[]")
}

func TestFullSQLLogForExecuteHasParameterAndDisableTenant(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": false}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	rawSQL := "select * from project_construction_quantity_contract_bill_part where id = ? and progress_item_name = ?"
	ctx := context.Background()
	// simple prepare and execute
	data := append([]byte{mysql.ComStmtPrepare}, []byte(rawSQL)...)
	require.NoError(t, cc.Dispatch(ctx, data))
	data = []byte{
		mysql.ComStmtExecute, // cmd
		0x01, 0x0, 0x0, 0x0,  // stmtID little endian
		0x00,               // useCursor == false
		0x1, 0x0, 0x0, 0x0, // iteration-count, always 1
		0x0,     // 表示空值的位信息。一个字节可以表示8个参数的可空信息
		0x1,     // new-params-bound-flag
		0x8, 00, // bigint type
		0xfe, 00, // string type
		0x0A, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // bigint value 10
		0xfc, 0x4, 0x0, 'a', 'b', 'c', 'd', // string value 'abcd'
	}
	require.NoError(t, cc.Dispatch(ctx, data))
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)
	sql := logData["sql"]
	require.Contains(t, sql, rawSQL+";Type=Binary;Params=[10,'abcd']")

	// -------------------------------------------------------------------------------------

	tk.MustExec("prepare stmt0 from '" + rawSQL + "'")
	tk.MustExec("set @p0=50,@p1='ABCD'")
	tk.MustQuery("execute stmt0 using @p0,@p1")
	logData, err = interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)
	sql = logData["sql"]
	require.Contains(t, sql, rawSQL+";Type=SQL;Name=stmt0;Params=[50,'ABCD']")
}

func TestFullSQLLogForExecuteNoParameterAndEnableTenant(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	rawSQL := "select * from project_construction_quantity_contract_bill_part"
	ctx := context.Background()
	data := append([]byte{mysql.ComStmtPrepare}, []byte(rawSQL)...)
	require.NoError(t, cc.Dispatch(ctx, data))
	data = []byte{
		mysql.ComStmtExecute, // cmd
		0x01, 0x0, 0x0, 0x0,  // stmtID little endian
		0x00,               // useCursor == false
		0x1, 0x0, 0x0, 0x0, // iteration-count, always 1
		0x0,      // 表示空值的位信息。一个字节可以表示8个参数的可空信息
		0x1,      // new-params-bound-flag
		0xfe, 00, // string type
		0xfc, 0x4, 0x0, 'g', 's', 'l', 'q', // string value 'gslq'
	}
	require.NoError(t, cc.Dispatch(ctx, data))
	logData1, err1 := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err1)
	require.NotNil(t, logData1)
	sql1 := logData1["sql"]
	require.Contains(t, sql1, rawSQL+";Type=Binary;Params=['gslq']")

	// -------------------------------------------------------------------------------------

	tk.MustExec("prepare stmt0 from '" + rawSQL + "'")
	tk.MustExec("set @p0='sxlq'")
	tk.MustQuery("execute stmt0 using @p0")
	logData2, err2 := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err2)
	require.NotNil(t, logData2)
	sql2 := logData2["sql"]
	require.Contains(t, sql2, rawSQL+";Type=SQL;Name=stmt0;Params=['sxlq']")
}

func TestFullSQLLogForExecuteHasParameterAndEnableTenant(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	rawSQL := "select * from project_construction_quantity_contract_bill_part where id = ? and progress_item_name = ?"
	ctx := context.Background()
	data := append([]byte{mysql.ComStmtPrepare}, []byte(rawSQL)...)
	require.NoError(t, cc.Dispatch(ctx, data))
	data = []byte{
		mysql.ComStmtExecute, // cmd
		0x01, 0x0, 0x0, 0x0,  // stmtID little endian
		0x00,               // useCursor == false
		0x1, 0x0, 0x0, 0x0, // iteration-count, always 1
		0x0,     // 表示空值的位信息。一个字节可以表示8个参数的可空信息
		0x1,     // new-params-bound-flag
		0x8, 00, // bigint type
		0xfe, 00, // string type
		0xfe, 00, // string type
		0x0A, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // bigint value 10
		0xfc, 0x4, 0x0, 'a', 'b', 'c', 'd', // string value 'abcd'
		0xfc, 0x4, 0x0, 'g', 's', 'l', 'q', // string value 'gslq'
	}

	require.NoError(t, cc.Dispatch(ctx, data))
	logData1, err1 := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err1)
	require.NotNil(t, logData1)
	sql1 := logData1["sql"]
	require.Contains(t, sql1, rawSQL+";Type=Binary;Params=[10,'abcd','gslq']")

	// -------------------------------------------------------------------------------------

	tk.MustExec("prepare stmt0 from '" + rawSQL + "'")
	tk.MustExec("set @p0=50,@p1='ABCD',@p3='sxlq'")
	tk.MustQuery("execute stmt0 using @p0,@p1,@p3")
	logData2, err2 := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err2)
	require.NotNil(t, logData2)
	sql2 := logData2["sql"]
	require.Contains(t, sql2, rawSQL+";Type=SQL;Name=stmt0;Params=[50,'ABCD','sxlq']")
}
