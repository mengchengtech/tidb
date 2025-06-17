// add by zhangbing

package variable_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

type testLargeQueryFormatCase struct {
	dbChanged bool
	succ      bool
	sqlType   string
	parse     time.Duration
	compile   time.Duration
	optimize  time.Duration
	render    time.Duration
	sql       string
	memMax    int64
	diskMax   int64
	results   int64
	fields    map[string]string
}

var fieldNames = []string{
	"# TIME", "# USER@HOST", "# QUERY_TIME", "# PARSE_TIME", "# COMPILE_TIME", "# REWRITE_TIME",
	"# OPTIMIZE_TIME", "{COMPAT_DATA}", "# DB", "# DIGEST", "# MEM_MAX", "# DISK_MAX", "# RESULT_ROWS",
	"# SUCC", "# SQL_LENGTH", "# SQL_TYPE", "# APP_NAME", "# PRODUCT_LINE", "# PACKAGE", "# PLAN",
	"{SQL}", // sql
}

func TestLargeQueryFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_ec3")
	tk.MustExec("create database global_ec3")
	tk.MustExec("create table global_ec3.t (id bigint)")
	sess := tk.Session()

	seVar := sess.GetSessionVars()
	require.NotNil(t, seVar)

	seVar.User = &auth.UserIdentity{Username: "root", Hostname: "192.168.0.1"}
	seVar.ConnectionInfo = &variable.ConnectionInfo{ClientIP: "192.168.0.1"}
	seVar.CurrentDB = "global_ec3"

	ctx := context.Background()
	cases := []testLargeQueryFormatCase{
		{false, true, "select", 10, 10, 10, 5, "select * from t;", 2333, 6666, 12345, map[string]string{
			"# DIGEST": "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7",
			"{SQL}":    "{gzip}H4sIAAAAAAAA/ypOzUlNLlHQUkgrys9VKLEGBAAA///MPyzQEAAAAA==;",
		}},
		{true, true, "select", 10, 10, 10, 5, "/* from:'org-service' */ select * from t;", 3444, 6666, 12345, map[string]string{
			"# DIGEST":   "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7",
			"# APP_NAME": "org-service",
			"{SQL}":      "use global_ec3;\n{gzip}H4sIAAAAAAAA/9LXUkgrys+1Us8vStctTi0qy0xOVVfQ0lcoTs1JTS5RgEgrlFgDAgAA//88xCBTKQAAAA==;",
		}},
		{true, true, "select", 10, 10, 10, 5, "/* from:'org-service.pf' */ /* package:'@mctech/dp-impala' */ select * from t", 4444, 5666, 22, map[string]string{
			"# DIGEST":       "e5796985ccafe2f71126ed6c0ac939ffa015a8c0744a24b7aee6d587103fd2f7",
			"# APP_NAME":     "org-service",
			"# PRODUCT_LINE": "pf",
			"# PACKAGE":      "@mctech/dp-impala",
			"{SQL}":          "use global_ec3;\n{gzip}H4sIAAAAAAAA/9LXUkgrys+1Us8vStctTi0qy0xO1StIU1fQ0lfQ11IoSEzOTkxPtVJ3yE0uSU3O0E8p0M3MLUjMSQSrKE7NSU0uUYCYoVACCAAA///fVtAZTQAAAA==;",
		}},
	}

	for _, c := range cases {
		seVar.CurrentDBChanged = c.dbChanged
		n, err := sess.Parse(context.Background(), c.sql)
		require.NoError(t, err, c.sql)
		executor.ResetContextOfStmt(sess, n[0])
		seVar.DurationParse = time.Duration(10)
		seVar.DurationCompile = time.Duration(10)
		seVar.DurationOptimization = time.Duration(10)
		seVar.RewritePhaseInfo = variable.RewritePhaseInfo{DurationRewrite: 3}

		stmtCtx := seVar.StmtCtx
		stmtCtx.MergeExecDetails(&execdetails.ExecDetails{
			DetailsNeedP90: execdetails.DetailsNeedP90{
				TimeDetail: util.TimeDetail{
					ProcessTime: time.Second * time.Duration(2),
					WaitTime:    time.Minute,
				},
			},
			ScanDetail: &util.ScanDetail{TotalKeys: 10000},
		}, nil)
		stmtCtx.SetEncodedPlan(c.sql)
		ctx := context.WithValue(ctx, execdetails.StmtExecDetailKey, &execdetails.StmtExecDetails{
			WriteSQLRespDuration: c.render,
		})
		stmtCtx.MemTracker.Consume(c.memMax)
		stmtCtx.DiskTracker.Consume(c.diskMax)
		comments := mctech.GetCustomCommentFromSQL(c.sql)
		logItems := executor.CreateLargeQueryItems(ctx, c.sql, c.sqlType, c.succ, c.results, seVar, comments)
		logItems.TimeTotal = time.Second

		logString, err := seVar.LargeQueryFormat(logItems)
		require.NoError(t, err)

		fullFields := map[string]string{
			"# USER@HOST":  "root[root] @ 192.168.0.1 [192.168.0.1]",
			"# QUERY_TIME": "1", "# PARSE_TIME": "0.00000001", "# COMPILE_TIME": "0.00000001", "# REWRITE_TIME": "0.000000003", "# OPTIMIZE_TIME": "0.00000001",
			"{COMPAT_DATA}": "PROCESS_TIME: 2 WAIT_TIME: 60 TOTAL_KEYS: 10000",
			"# DB":          "global_ec3",
			"# MEM_MAX":     strconv.FormatInt(c.memMax, 10),
			"# DISK_MAX":    strconv.FormatInt(c.diskMax, 10),
			"# RESULT_ROWS": strconv.FormatInt(c.results, 10),
			"# SUCC":        strconv.FormatBool(c.succ),
			"# SQL_LENGTH":  strconv.Itoa(len(c.sql)),
			"# SQL_TYPE":    c.sqlType,
			"# PLAN":        fmt.Sprintf("tidb_decode_plan('%s')", c.sql),
		}

		for k, v := range c.fields {
			fullFields[k] = v
		}

		list := make([]string, 0, len(fieldNames))
		for _, k := range fieldNames {
			if v, ok := fullFields[k]; ok && len(v) > 0 {
				txt := v
				switch k {
				case "{SQL}":
					break
				case "{COMPAT_DATA}":
					txt = "# " + txt
				default:
					txt = fmt.Sprintf("%s: %s", k, v)
				}
				list = append(list, txt)
			}
		}
		text := strings.Join(list, "\n")
		require.Equal(t, text, logString, c.sql)
	}
}
