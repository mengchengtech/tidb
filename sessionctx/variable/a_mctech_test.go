package variable_test

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"golang.org/x/exp/slices"
)

func TestLargeQueryFormat(t *testing.T) {
	ctx := mock.NewContext()

	seVar := ctx.GetSessionVars()
	require.NotNil(t, seVar)

	seVar.User = &auth.UserIdentity{Username: "root", Hostname: "192.168.0.1"}
	seVar.ConnectionInfo = &variable.ConnectionInfo{ClientIP: "192.168.0.1"}
	seVar.CurrentDB = "TeST"
	costTime := time.Second
	execDetail := execdetails.ExecDetails{
		DetailsNeedP90: execdetails.DetailsNeedP90{
			TimeDetail: util.TimeDetail{
				ProcessTime: time.Second * time.Duration(2),
				WaitTime:    time.Minute,
			},
		},
		ScanDetail: &util.ScanDetail{
			TotalKeys: 10000,
		},
	}
	statsInfos := make(map[string]uint64)
	statsInfos["t1"] = 0

	var memMax int64 = 2333
	var diskMax int64 = 6666
	resultFields := []string{
		"# USER@HOST: root[root] @ 192.168.0.1 [192.168.0.1]",
		"# QUERY_TIME: 1",
		"# PARSE_TIME: 0.00000001",
		"# COMPILE_TIME: 0.00000001",
		"# REWRITE_TIME: 0.000000003",
		"# OPTIMIZE_TIME: 0.00000001",
		"# PROCESS_TIME: 2 WAIT_TIME: 60 TOTAL_KEYS: 10000",
		"# DB: test",
		"# DIGEST: 01d00e6e93b28184beae487ac05841145d2a2f6a7b16de32a763bed27967e83d",
		"# MEM_MAX: 2333",
		"# DISK_MAX: 6666",
		"# RESULT_ROWS: 12345",
		"# SUCC: true",
	}
	sql := "select * from t;"
	_, digest := parser.NormalizeDigest(sql)
	logItems := &variable.MCTechLargeQueryLogItems{
		Digest:            digest.String(),
		TimeTotal:         costTime,
		TimeParse:         time.Duration(10),
		TimeCompile:       time.Duration(10),
		TimeOptimize:      time.Duration(10),
		ExecDetail:        execDetail,
		MemMax:            memMax,
		DiskMax:           diskMax,
		WriteSQLRespTotal: 1 * time.Second,
		ResultRows:        12345,
		Succ:              true,
		RewriteInfo: variable.RewritePhaseInfo{
			DurationRewrite: 3,
		},
	}

	logItems.SQL = sql
	logItems.Service = executor.GetSeriveFromSQL(sql)
	logString, err := seVar.LargeQueryFormat(logItems)
	require.NoError(t, err)
	text := strings.Join(append(
		slices.Clone(resultFields),
		"# SQL_LENGTH: 16",
		"{gzip}H4sIAAAAAAAA/ypOzUlNLlHQUkgrys9VKLEGBAAA///MPyzQEAAAAA==;",
	), "\n")
	require.Equal(t, text, logString)

	sql = "/* from:'org-service' */ " + sql
	logItems.SQL = sql
	logItems.Service = executor.GetSeriveFromSQL(sql)
	seVar.CurrentDBChanged = true
	logString, err = seVar.LargeQueryFormat(logItems)
	require.NoError(t, err)

	text = strings.Join(append(
		slices.Clone(resultFields),
		"# SQL_LENGTH: 41",
		"# SERVICE: org-service",
		"use test;",
		"{gzip}H4sIAAAAAAAA/9LXUkgrys+1Us8vStctTi0qy0xOVVfQ0lcoTs1JTS5RgEgrlFgDAgAA//88xCBTKQAAAA==;",
	), "\n")
	require.Equal(t, text, logString)
	require.False(t, seVar.CurrentDBChanged)
}
