package interceptor_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/mctech/interceptor"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestFullSQLLogForExecuteNoParameterAndDisableTenant(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": false}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
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
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": false}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
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
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
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
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
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

func TestPrepareResultToExecuteBinary(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(151300), "disk": int64(9527), "rows": int64(1024), "maxAct": int64(2024),
		"rru": int(1111), "wru": int(22),
		"err": "mock sql error",
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	comment := "/* from:'ec-analysis-service' */ /*& across:global_sq,global_qa */\n"
	rawSQL := comment + "select * from project_construction_quantity_contract_bill_part"
	ctx := context.Background()
	// simple prepare and execute
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

	sessVars := tk.Session().GetSessionVars()
	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "tenant": "gslq", "across": "global_sq|global_qa",
		"tables": []any{
			map[string]any{"db": "global_ec3", "table": "project_construction_quantity_contract_bill_part"},
		},
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "ec-analysis-service", "product": "",
		},
		"cat": "exec", "tp": "select", "inTX": false, "maxAct": float64(2024),
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
		},
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"ru":     map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem":    float64(151300), "disk": float64(9527), "rows": float64(1024),
		"digest": "ecebafb52068500423b3b89b5549eaf2ad85da965cf108c495cc61fe641a394a",
		"sql":    rawSQL + ";Type=Binary;Params=['gslq']",
		"error":  "mock sql error",
	}, logData1)
}

func TestPrepareResultToExecuteText(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(151300), "disk": int64(9527), "rows": int64(1024), "maxAct": int64(2024),
		"rru": int(1111), "wru": int(22),
		"err": "mock sql error",
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	comment := "/* from:'ec-analysis-service' */ /*& across:global_sq,global_qa */\n"
	rawSQL := "select * from project_construction_quantity_contract_bill_part"

	tk.MustExec(comment + "prepare stmt0 from '" + rawSQL + "'")
	tk.MustExec("set @p0='gslq'")
	tk.MustQuery(comment + "execute stmt0 using @p0")
	logData1, err1 := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err1)
	require.NotNil(t, logData1)
	sql2 := logData1["sql"]
	require.Contains(t, sql2, rawSQL+";Type=SQL;Name=stmt0;Params=['gslq']")

	sessVars := tk.Session().GetSessionVars()
	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "tenant": "gslq", "across": "global_sq|global_qa",
		"tables": []any{
			map[string]any{"db": "global_ec3", "table": "project_construction_quantity_contract_bill_part"},
		},
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "ec-analysis-service", "product": "",
		},
		"cat": "exec", "tp": "select", "inTX": false, "maxAct": float64(2024),
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
		},
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"ru":     map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem":    float64(151300), "disk": float64(9527), "rows": float64(1024),
		"digest": "ecebafb52068500423b3b89b5549eaf2ad85da965cf108c495cc61fe641a394a",
		"sql":    rawSQL + ";Type=SQL;Name=stmt0;Params=['gslq']",
		"error":  "mock sql error",
	}, logData1)
}

// -----------------------------------------------------------------------------------------

func TestBinaryPrepare(t *testing.T) {
	t.Skipf("do not record prepare sql")

	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(151300), "disk": int64(9527), "rows": int64(1024), "maxAct": int64(2024),
		"rru": int(1111), "wru": int(22),
		"err": "mock sql error",
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	comment := "/* from:'ec-analysis-service' */ /*& across:global_sq,global_qa */\n"
	rawSQL := comment + "select * from project_construction_quantity_contract_bill_part"
	ctx := context.Background()
	// simple prepare and execute
	data := append([]byte{mysql.ComStmtPrepare}, []byte(rawSQL)...)
	require.NoError(t, cc.Dispatch(ctx, data))
	logData1, err1 := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err1)
	require.NotNil(t, logData1)

	sessVars := tk.Session().GetSessionVars()
	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "across": "global_sq|global_qa",
		"tables": []any{
			map[string]any{"db": "global_ec3", "table": "project_construction_quantity_contract_bill_part"},
		},
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "ec-analysis-service", "product": "",
		},
		"cat": "exec", "tp": "prepare", "inTX": false, "maxAct": float64(2024),
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
		},
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"ru":     map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem":    float64(151300), "disk": float64(9527), "rows": float64(1024),
		"digest": "ecebafb52068500423b3b89b5549eaf2ad85da965cf108c495cc61fe641a394a",
		"sql":    rawSQL + ";Type=Binary",
		"error":  "mock sql error",
	}, logData1)
}

func TestTextPrepare(t *testing.T) {
	t.Skipf("do not record prepare sql")

	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(151300), "disk": int64(9527), "rows": int64(1024), "maxAct": int64(2024),
		"rru": int(1111), "wru": int(22),
		"err": "mock sql error",
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()

	extension.Reset()
	mctech.RegisterExtensions()
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	srv := server.CreateMockServer(t, store)
	cc := server.CreateMockConn(t, srv)
	cc.Context().Session = tk.Session()

	comment := "/* from:'ec-analysis-service' */ /*& across:global_sq,global_qa */\n"
	rawSQL := "select * from project_construction_quantity_contract_bill_part"

	tk.MustExec(comment + "prepare stmt0 from '" + rawSQL + "'")
	logData1, err1 := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err1)
	require.NotNil(t, logData1)

	sessVars := tk.Session().GetSessionVars()
	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "across": "global_sq|global_qa",
		"tables": []any{
			map[string]any{"db": "global_ec3", "table": "project_construction_quantity_contract_bill_part"},
		},
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "ec-analysis-service", "product": "",
		},
		"cat": "exec", "tp": "prepare", "inTX": false, "maxAct": float64(2024),
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
		},
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"ru":     map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem":    float64(151300), "disk": float64(9527), "rows": float64(1024),
		"digest": "ecebafb52068500423b3b89b5549eaf2ad85da965cf108c495cc61fe641a394a",
		"sql":    rawSQL + ";Type=SQL;Name=stmt0",
		"error":  "mock sql error",
	}, logData1)
}
