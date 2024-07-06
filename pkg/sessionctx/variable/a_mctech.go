// add by zhangbing

package variable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"go.uber.org/zap"
)

const (
	MCLargeQueryRowPrefixStr = "# "
	// MCLargeQuerySpaceMarkStr is large query space mark.
	MCLargeQuerySpaceMarkStr   = ": "
	MCLargeQuerySQLSuffixStr   = ";"
	MCLargeQueryStartPrefixStr = MCLargeQueryRowPrefixStr + MCLargeQueryTimeStr + MCLargeQuerySpaceMarkStr
	// MCLargeQueryUserAndHostStr is the user and host field name, which is compatible with MySQL.
	MCLargeQueryUserAndHostStr = "USER@HOST"

	MCLargeQueryTimeStr         = "TIME"
	MCLargeQueryUserStr         = "USER"
	MCLargeQueryHostStr         = "HOST"
	MCLargeQueryQueryTimeStr    = "QUERY_TIME"
	MCLargeQueryParseTimeStr    = "PARSE_TIME"
	MCLargeQueryCompileTimeStr  = "COMPILE_TIME"
	MCLargeQueryRewriteTimeStr  = "REWRITE_TIME"
	MCLargeQueryOptimizeTimeStr = "OPTIMIZE_TIME"

	MCLargeQueryDBStr      = "DB"
	MCLargeQuerySQLStr     = "SQL"
	MCLargeQuerySuccStr    = "SUCC"
	MCLargeQueryMemMax     = "MEM_MAX"
	MCLargeQueryDiskMax    = "DISK_MAX"
	MCLargeQueryDigestStr  = "DIGEST"
	MCLargeQueryResultRows = "RESULT_ROWS"
	MCLargeQueryPlan       = "PLAN"
)

const (
	MCTechSequenceMaxFetchCount = "mctech_sequence_max_fetch_count"
	MCTechSequenceBackend       = "mctech_sequence_backend"

	MCTechDbCheckerEnabled = "mctech_db_checker_enabled"

	MCTechTenantEnabled          = "mctech_tenant_enabled"
	MCTechTenantForbiddenPrepare = "mctech_tenant_forbidden_prepare"

	MCTechDDLVersionEnabled   = "mctech_ddl_version_enabled"
	MCTechDDLVersionName      = "mctech_ddl_version_name"
	MCTechDDLVersionDbMatches = "mctech_ddl_version_db_matches"

	MCTechMPPDefaultValue = "mctech_mpp_default_value"

	MCTechMetricsLargeQueryEnabled     = "mctech_metrics_large_query_enabled"
	MCTechMetricsLargeQueryFilename    = "mctech_metrics_large_query_file"
	MCTechMetricsLargeQueryFileMaxDays = "mctech_metrics_large_query_file_max_days"
	MCTechMetricsLargeQueryFileMaxSize = "mctech_metrics_large_query_file_max_size"
	MCTechMetricsLargeQueryTypes       = "mctech_metrics_large_query_types"
	MCTechMetricsLargeQueryThreshold   = "mctech_metrics_large_query_threshold"

	MCTechMetricsQueryLogEnabled   = "mctech_metrics_query_log_enabled"
	MCTechMetricsQueryLogMaxLength = "mctech_metrics_query_log_max_length"

	MCTechMetricsSqlTraceEnabled           = "mctech_metrics_sql_trace_enabled"
	MCTechMetricsSqlTraceFilename          = "mctech_metrics_sql_trace_file"
	MCTechMetricsSqlTraceFileMaxSize       = "mctech_metrics_sql_trace_file_max_size"
	MCTechMetricsSqlTraceFileMaxDays       = "mctech_metrics_sql_trace_file_max_days"
	MCTechMetricsSqlTraceCompressThreshold = "mctech_metrics_sql_trace_compress_threshold"

	MCTechMetricsExcludeDbs = "mctech_metrics_exclude_dbs"
)

func init() {
	var mctechSysVars = []*SysVar{
		{Scope: ScopeNone, Name: MCTechSequenceMaxFetchCount, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSequenceMaxFetchCount)},
		{Scope: ScopeNone, Name: MCTechSequenceBackend, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSequenceBackend)},

		{Scope: ScopeNone, Name: MCTechDbCheckerEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultDbCheckerEnabled)},

		{Scope: ScopeNone, Name: MCTechTenantEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultTenantEnabled)},
		{Scope: ScopeNone, Name: MCTechTenantForbiddenPrepare, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultTenantForbiddenPrepare)},

		{Scope: ScopeNone, Name: MCTechDDLVersionEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultDDLVersionEnabled)},
		{Scope: ScopeNone, Name: MCTechDDLVersionName, skipInit: true, Type: TypeStr, Value: config.DefaultDDLVersionColumnName},
		{Scope: ScopeNone, Name: MCTechDDLVersionDbMatches, skipInit: true, Type: TypeStr, Value: config.DefaultDDLVersionDbMatches},

		{Scope: ScopeGlobal, Name: MCTechMPPDefaultValue, skipInit: true, Type: TypeEnum, Value: config.DefaultMPPValue,
			PossibleValues: []string{"allow", "force", "disable"},
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return config.GetMCTechConfig().MPP.DefaultValue, nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().MPP.DefaultValue = val
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsLargeQueryEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.LargeQuery.Enabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.LargeQuery.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechMetricsLargeQueryFilename, skipInit: true, Type: TypeBool, Value: config.DefaultMetricsLargeQueryFilename},
		{Scope: ScopeNone, Name: MCTechMetricsLargeQueryFileMaxDays, skipInit: true, Type: TypeBool, Value: strconv.Itoa(config.DefaultMetricsLargeQueryFileMaxDays)},
		{Scope: ScopeNone, Name: MCTechMetricsLargeQueryFileMaxSize, skipInit: true, Type: TypeBool, Value: strconv.Itoa(config.DefaultMetricsLargeQueryFileMaxSize)},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryTypes, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultAllowMetricsLargeQueryTypes, ","),
			Validation: func(vars *SessionVars, _ string, original string, scope ScopeFlag) (string, error) {
				return validateEnumSet(original, ",", config.DefaultAllowMetricsLargeQueryTypes)
			},
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().Metrics.LargeQuery.Types, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.LargeQuery.Types = config.StrToSlice(val, ",")
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsLargeQueryThreshold),
			MinValue: 4 * 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.LargeQuery.Threshold), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.LargeQuery.Threshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsQueryLogEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsQueryLogEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.QueryLog.Enabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.QueryLog.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsQueryLogMaxLength, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsQueryLogMaxLength),
			MinValue: 16 * 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.QueryLog.MaxLength), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.QueryLog.MaxLength = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlTraceEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsSqlTraceEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.SqlTrace.Enabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.SqlTrace.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechMetricsSqlTraceFilename, skipInit: true, Type: TypeBool, Value: config.DefaultMetricsSqlTraceFilename},
		{Scope: ScopeNone, Name: MCTechMetricsSqlTraceFileMaxSize, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsSqlTraceFileMaxSize)},
		{Scope: ScopeNone, Name: MCTechMetricsSqlTraceFileMaxDays, skipInit: true, Type: TypeStr, Value: strconv.Itoa(config.DefaultMetricsSqlTraceFileMaxDays)},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlTraceCompressThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsSqlTraceCompressThreshold),
			MinValue: 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.SqlTrace.CompressThreshold), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}

				config.GetMCTechConfig().Metrics.SqlTrace.CompressThreshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsExcludeDbs, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultSqlTraceExcludeDbs, ","),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().Metrics.Exclude, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.Exclude = config.StrToSlice(val, ",")
				return nil
			},
		},
	}

	defaultSysVars = append(defaultSysVars, mctechSysVars...)
}

func validateEnumSet(input string, sep string, possibleValues []string) (string, error) {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return "", nil
	}

	var result []string
	parts := strings.Split(s, sep)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) == 0 || slices.Contains(result, part) {
			continue
		}
		if !slices.Contains(possibleValues, part) {
			return input, ErrWrongValueForVar.GenWithStackByArgs(MCTechMetricsLargeQueryTypes, input)
		}

		result = append(result, part)
	}
	return strings.Join(result, sep), nil
}

func LoadMctechSysVars() {
	option := config.GetMCTechConfig()
	bytes, err := json.Marshal(option)
	if err != nil {
		panic(err)
	}
	log.Warn("LoadMctechSysVars", zap.ByteString("config", bytes))

	SetSysVar(MCTechSequenceMaxFetchCount, strconv.FormatInt(option.Sequence.MaxFetchCount, 10))
	SetSysVar(MCTechSequenceBackend, strconv.FormatInt(option.Sequence.Backend, 10))

	SetSysVar(MCTechDbCheckerEnabled, BoolToOnOff(option.DbChecker.Enabled))

	SetSysVar(MCTechTenantEnabled, BoolToOnOff(option.Tenant.Enabled))
	SetSysVar(MCTechTenantForbiddenPrepare, BoolToOnOff(option.Tenant.ForbiddenPrepare))

	SetSysVar(MCTechDDLVersionEnabled, BoolToOnOff(option.DDL.Version.Enabled))
	SetSysVar(MCTechDDLVersionName, option.DDL.Version.Name)
	SetSysVar(MCTechDDLVersionDbMatches, strings.Join(option.DDL.Version.DbMatches, ","))

	SetSysVar(MCTechMPPDefaultValue, option.MPP.DefaultValue)

	SetSysVar(MCTechMetricsQueryLogEnabled, BoolToOnOff(option.Metrics.QueryLog.Enabled))
	SetSysVar(MCTechMetricsQueryLogMaxLength, strconv.Itoa(option.Metrics.QueryLog.MaxLength))

	SetSysVar(MCTechMetricsLargeQueryEnabled, BoolToOnOff(option.Metrics.LargeQuery.Enabled))
	SetSysVar(MCTechMetricsLargeQueryTypes, strings.Join(option.Metrics.LargeQuery.Types, ","))
	SetSysVar(MCTechMetricsLargeQueryThreshold, strconv.Itoa(option.Metrics.LargeQuery.Threshold))
	SetSysVar(MCTechMetricsLargeQueryFilename, option.Metrics.LargeQuery.Filename)
	SetSysVar(MCTechMetricsLargeQueryFileMaxDays, strconv.Itoa(option.Metrics.LargeQuery.FileMaxDays))
	SetSysVar(MCTechMetricsLargeQueryFileMaxSize, strconv.Itoa(option.Metrics.LargeQuery.FileMaxSize))

	SetSysVar(MCTechMetricsSqlTraceEnabled, BoolToOnOff(option.Metrics.SqlTrace.Enabled))
	SetSysVar(MCTechMetricsSqlTraceFilename, option.Metrics.SqlTrace.Filename)
	SetSysVar(MCTechMetricsSqlTraceFileMaxSize, strconv.Itoa(option.Metrics.SqlTrace.FileMaxSize))
	SetSysVar(MCTechMetricsSqlTraceFileMaxDays, strconv.Itoa(option.Metrics.SqlTrace.FileMaxDays))
	SetSysVar(MCTechMetricsSqlTraceCompressThreshold, strconv.Itoa(option.Metrics.SqlTrace.CompressThreshold))

	SetSysVar(MCTechMetricsExcludeDbs, strings.Join(option.Metrics.Exclude, ","))
}

// MCLargeQueryItems is a collection of items that should be included in the
type MCLargeQueryItems struct {
	Digest            string
	TimeTotal         time.Duration
	TimeParse         time.Duration
	TimeCompile       time.Duration
	TimeOptimize      time.Duration
	MemMax            int64
	DiskMax           int64
	RewriteInfo       RewritePhaseInfo
	ExecDetail        execdetails.ExecDetails
	Plan              string
	WriteSQLRespTotal time.Duration
	ResultRows        int64
	Succ              bool
	SQL               string
}

// # TIME: 2019-04-28T15:24:04.309074+08:00
// # USER@HOST: root[root] @ localhost [127.0.0.1]
// # QUERY_TIME: 1.527627037
// # PARSE_TIME: 0.000054933
// # COMPILE_TIME: 0.000129729
// # REWRITE_TIME: 0.000000003
// # OPTIMIZE_TIME: 0.00000001
// # PROCESS_TIME: 0.07
// # TOTAL_KEYS: 131073
// # WRITE_KEYS: 131072
// # WRITE_SIZE: 3538944
// # DB: test
// # DIGEST: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
// # MEMORY_MAX: 4096
// # DISK_MAX: 65535
// # Succ: true
// # COP_TIME:
// # WAIT_TIME: 0
// # RESULT_ROWS: 1
// # Plan: tidb_decode_plan('ZJAwCTMyXzcJMAkyMAlkYXRhOlRhYmxlU2Nhbl82CjEJMTBfNgkxAR0AdAEY1Dp0LCByYW5nZTpbLWluZiwraW5mXSwga2VlcCBvcmRlcjpmYWxzZSwgc3RhdHM6cHNldWRvCg==')
// use test;
// insert into t select * from t;
func (s *SessionVars) LargeQueryFormat(logItems *MCLargeQueryItems) string {
	var buf bytes.Buffer

	if s.User != nil {
		hostAddress := s.User.Hostname
		if s.ConnectionInfo != nil {
			hostAddress = s.ConnectionInfo.ClientIP
		}
		writeSlowLogItem(&buf, MCLargeQueryUserAndHostStr, fmt.Sprintf("%s[%s] @ %s [%s]", s.User.Username, s.User.Username, s.User.Hostname, hostAddress))
	}
	writeSlowLogItem(&buf, MCLargeQueryQueryTimeStr, strconv.FormatFloat(logItems.TimeTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, MCLargeQueryParseTimeStr, strconv.FormatFloat(logItems.TimeParse.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, MCLargeQueryCompileTimeStr, strconv.FormatFloat(logItems.TimeCompile.Seconds(), 'f', -1, 64))

	buf.WriteString(MCLargeQueryRowPrefixStr + fmt.Sprintf("%v%v%v", MCLargeQueryRewriteTimeStr,
		MCLargeQuerySpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationRewrite.Seconds(), 'f', -1, 64)))
	buf.WriteString("\n")

	writeSlowLogItem(&buf, MCLargeQueryOptimizeTimeStr, strconv.FormatFloat(logItems.TimeOptimize.Seconds(), 'f', -1, 64))

	if execDetailStr := logItems.ExecDetail.LargeQueryString(); len(execDetailStr) > 0 {
		buf.WriteString(MCLargeQueryRowPrefixStr + execDetailStr + "\n")
	}

	if len(s.CurrentDB) > 0 {
		writeSlowLogItem(&buf, MCLargeQueryDBStr, strings.ToLower(s.CurrentDB))
	}

	if len(logItems.Digest) > 0 {
		writeSlowLogItem(&buf, MCLargeQueryDigestStr, logItems.Digest)
	}
	if logItems.MemMax > 0 {
		writeSlowLogItem(&buf, MCLargeQueryMemMax, strconv.FormatInt(logItems.MemMax, 10))
	}
	if logItems.DiskMax > 0 {
		writeSlowLogItem(&buf, MCLargeQueryDiskMax, strconv.FormatInt(logItems.DiskMax, 10))
	}

	writeSlowLogItem(&buf, MCLargeQueryResultRows, strconv.FormatInt(logItems.ResultRows, 10))
	writeSlowLogItem(&buf, MCLargeQuerySuccStr, strconv.FormatBool(logItems.Succ))

	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, MCLargeQueryPlan, logItems.Plan)
	}

	if s.CurrentDBChanged {
		buf.WriteString(fmt.Sprintf("use %s;\n", strings.ToLower(s.CurrentDB)))
		s.CurrentDBChanged = false
	}

	buf.WriteString(logItems.SQL)
	if len(logItems.SQL) == 0 || logItems.SQL[len(logItems.SQL)-1] != ';' {
		buf.WriteString(";")
	}
	return buf.String()
}
