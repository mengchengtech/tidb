// add by zhangbing

package variable

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	MCLargeLogRowPrefixStr = "# "
	// MCLargeLogSpaceMarkStr is large log space mark.
	MCLargeLogSpaceMarkStr   = ": "
	MCLargeLogSQLSuffixStr   = ";"
	MCLargeLogGzipPrefixStr  = "{gzip}"
	MCLargeLogStartPrefixStr = MCLargeLogRowPrefixStr + MCLargeLogTimeStr + MCLargeLogSpaceMarkStr
	// MCLargeLogUserAndHostStr is the user and host field name, which is compatible with MySQL.
	MCLargeLogUserAndHostStr = "USER@HOST"

	MCLargeLogTimeStr         = "TIME"
	MCLargeLogUserStr         = "USER"
	MCLargeLogHostStr         = "HOST"
	MCLargeLogQueryTimeStr    = "QUERY_TIME"
	MCLargeLogParseTimeStr    = "PARSE_TIME"
	MCLargeLogCompileTimeStr  = "COMPILE_TIME"
	MCLargeLogRewriteTimeStr  = "REWRITE_TIME"
	MCLargeLogOptimizeTimeStr = "OPTIMIZE_TIME"

	MCLargeLogDBStr      = "DB"
	MCLargeLogSQLStr     = "SQL"
	MCLargeLogSuccStr    = "SUCC"
	MCLargeLogMemMax     = "MEM_MAX"
	MCLargeLogDiskMax    = "DISK_MAX"
	MCLargeLogDigestStr  = "DIGEST"
	MCLargeLogResultRows = "RESULT_ROWS"
	MCLargeLogPlan       = "PLAN"
)

const (
	MCTechSequenceMaxFetchCount = "mctech_sequence_max_fetch_count"
	MCTechSequenceBackend       = "mctech_sequence_backend"

	MCTechDbCheckerEnabled    = "mctech_db_checker_enabled"
	MCTechDbCheckerCompatible = "mctech_db_checker_compatible"

	MCTechDbCheckerMutexDbs   = "mctech_checker_mutex_dbs"
	MCTechDbCheckerExcludeDbs = "mctech_checker_exclude_dbs"
	MCTechDbCheckerDbGroups   = "mctech_db_checker_db_groups"

	MCTechTenantEnabled          = "mctech_tenant_enabled"
	MCTechTenantForbiddenPrepare = "mctech_tenant_forbidden_prepare"

	MCTechDDLVersionEnabled   = "mctech_ddl_version_enabled"
	MCTechDDLVersionName      = "mctech_ddl_version_name"
	MCTechDDLVersionDbMatches = "mctech_ddl_version_db_matches"

	MCTechMPPDefaultValue = "mctech_mpp_default_value"

	MCTechMetricsLargeLogEnabled     = "mctech_metrics_large_log_enabled"
	MCTechMetricsLargeLogFilename    = "mctech_metrics_large_log_file"
	MCTechMetricsLargeLogFileMaxDays = "mctech_metrics_large_log_file_max_days"
	MCTechMetricsLargeLogFileMaxSize = "mctech_metrics_large_log_file_max_size"
	MCTechMetricsLargeLogTypes       = "mctech_metrics_large_log_types"
	MCTechMetricsLargeLogThreshold   = "mctech_metrics_large_log_threshold"

	MCTechMetricsSqlLogEnabled   = "mctech_metrics_sql_log_enabled"
	MCTechMetricsSqlLogMaxLength = "mctech_metrics_sql_log_max_length"

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
		{Scope: ScopeGlobal, Name: MCTechDbCheckerCompatible, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultDbCheckerCompatible),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().DbChecker.Compatible), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().DbChecker.Compatible = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechDbCheckerMutexDbs, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDbCheckerMutexDbs, ",")},
		{Scope: ScopeNone, Name: MCTechDbCheckerExcludeDbs, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDbCheckerExcludeDbs, ",")},
		{Scope: ScopeNone, Name: MCTechDbCheckerDbGroups, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDbCheckerDbGroups, "|")},

		{Scope: ScopeNone, Name: MCTechTenantEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultTenantEnabled)},
		{Scope: ScopeNone, Name: MCTechTenantForbiddenPrepare, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultTenantForbiddenPrepare)},

		{Scope: ScopeNone, Name: MCTechDDLVersionEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultDDLVersionEnabled)},
		{Scope: ScopeNone, Name: MCTechDDLVersionName, skipInit: true, Type: TypeStr, Value: config.DefaultDDLVersionColumnName},
		{Scope: ScopeNone, Name: MCTechDDLVersionDbMatches, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDDLVersionDbMatches, ",")},

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
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeLogEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsLargeLogEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.LargeLog.Enabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.LargeLog.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechMetricsLargeLogFilename, skipInit: true, Type: TypeBool, Value: config.DefaultMetricsLargeLogFilename},
		{Scope: ScopeNone, Name: MCTechMetricsLargeLogFileMaxDays, skipInit: true, Type: TypeBool, Value: strconv.Itoa(config.DefaultMetricsLargeLogFileMaxDays)},
		{Scope: ScopeNone, Name: MCTechMetricsLargeLogFileMaxSize, skipInit: true, Type: TypeBool, Value: strconv.Itoa(config.DefaultMetricsLargeLogFileMaxSize)},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeLogTypes, skipInit: true, Type: TypeStr, Value: config.DefaultMetricsLargeLogTypes,
			Validation: func(vars *SessionVars, _ string, original string, scope ScopeFlag) (string, error) {
				return validateEnumSet(original, ",", config.AllAllowMetricsLargeLogTypes)
			},
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().Metrics.LargeLog.SqlTypes, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.LargeLog.SqlTypes = config.StrToSlice(val, ",")
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeLogThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsLargeLogThreshold),
			MinValue: 4 * 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.LargeLog.Threshold), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.LargeLog.Threshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlLogEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsSqlLogEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.SqlLog.Enabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.SqlLog.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlLogMaxLength, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsSqlLogMaxLength),
			MinValue: 16 * 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.SqlLog.MaxLength), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.SqlLog.MaxLength = num
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
			return input, ErrWrongValueForVar.GenWithStackByArgs(MCTechMetricsLargeLogTypes, input)
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
	SetSysVar(MCTechDbCheckerCompatible, BoolToOnOff(option.DbChecker.Compatible))
	SetSysVar(MCTechDbCheckerMutexDbs, strings.Join(option.DbChecker.MutexDbs, ","))
	SetSysVar(MCTechDbCheckerExcludeDbs, strings.Join(option.DbChecker.ExcludeDbs, ","))
	SetSysVar(MCTechDbCheckerDbGroups, strings.Join(option.DbChecker.DbGroups, ","))

	SetSysVar(MCTechTenantEnabled, BoolToOnOff(option.Tenant.Enabled))
	SetSysVar(MCTechTenantForbiddenPrepare, BoolToOnOff(option.Tenant.ForbiddenPrepare))

	SetSysVar(MCTechDDLVersionEnabled, BoolToOnOff(option.DDL.Version.Enabled))
	SetSysVar(MCTechDDLVersionName, option.DDL.Version.Name)
	SetSysVar(MCTechDDLVersionDbMatches, strings.Join(option.DDL.Version.DbMatches, ","))

	SetSysVar(MCTechMPPDefaultValue, option.MPP.DefaultValue)

	SetSysVar(MCTechMetricsSqlLogEnabled, BoolToOnOff(option.Metrics.SqlLog.Enabled))
	SetSysVar(MCTechMetricsSqlLogMaxLength, strconv.Itoa(option.Metrics.SqlLog.MaxLength))

	SetSysVar(MCTechMetricsLargeLogEnabled, BoolToOnOff(option.Metrics.LargeLog.Enabled))
	SetSysVar(MCTechMetricsLargeLogTypes, strings.Join(option.Metrics.LargeLog.SqlTypes, ","))
	SetSysVar(MCTechMetricsLargeLogThreshold, strconv.Itoa(option.Metrics.LargeLog.Threshold))
	SetSysVar(MCTechMetricsLargeLogFilename, option.Metrics.LargeLog.Filename)
	SetSysVar(MCTechMetricsLargeLogFileMaxDays, strconv.Itoa(option.Metrics.LargeLog.FileMaxDays))
	SetSysVar(MCTechMetricsLargeLogFileMaxSize, strconv.Itoa(option.Metrics.LargeLog.FileMaxSize))

	SetSysVar(MCTechMetricsSqlTraceEnabled, BoolToOnOff(option.Metrics.SqlTrace.Enabled))
	SetSysVar(MCTechMetricsSqlTraceFilename, option.Metrics.SqlTrace.Filename)
	SetSysVar(MCTechMetricsSqlTraceFileMaxSize, strconv.Itoa(option.Metrics.SqlTrace.FileMaxSize))
	SetSysVar(MCTechMetricsSqlTraceFileMaxDays, strconv.Itoa(option.Metrics.SqlTrace.FileMaxDays))
	SetSysVar(MCTechMetricsSqlTraceCompressThreshold, strconv.Itoa(option.Metrics.SqlTrace.CompressThreshold))

	SetSysVar(MCTechMetricsExcludeDbs, strings.Join(option.Metrics.Exclude, ","))
}

// MCLargeLogItems is a collection of items that should be included in the
type MCLargeLogItems struct {
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
// # COP_TIME: 0.17 PROCESS_TIME: 0.07 WAIT_TIME: 0 WRITE_KEYS: 131072 WRITE_SIZE: 3538944 TOTAL_KEYS: 131073
// # DB: test
// # DIGEST: 42a1c8aae6f133e934d4bf0147491709a8812ea05ff8819ec522780fe657b772
// # MEM_MAX: 4096
// # DISK_MAX: 65535
// # SUCC: true
// # RESULT_ROWS: 1
// # Plan: tidb_decode_plan('ZJAwCTMyXzcJMAkyMAlkYXRhOlRhYmxlU2Nhbl82CjEJMTBfNgkxAR0AdAEY1Dp0LCByYW5nZTpbLWluZiwraW5mXSwga2VlcCBvcmRlcjpmYWxzZSwgc3RhdHM6cHNldWRvCg==')
// use test;
// insert into t select * from t;
func (s *SessionVars) LargeLogFormat(logItems *MCLargeLogItems) (string, error) {
	var buf bytes.Buffer

	if s.User != nil {
		hostAddress := s.User.Hostname
		if s.ConnectionInfo != nil {
			hostAddress = s.ConnectionInfo.ClientIP
		}
		writeSlowLogItem(&buf, MCLargeLogUserAndHostStr, fmt.Sprintf("%s[%s] @ %s [%s]", s.User.Username, s.User.Username, s.User.Hostname, hostAddress))
	}
	writeSlowLogItem(&buf, MCLargeLogQueryTimeStr, strconv.FormatFloat(logItems.TimeTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, MCLargeLogParseTimeStr, strconv.FormatFloat(logItems.TimeParse.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, MCLargeLogCompileTimeStr, strconv.FormatFloat(logItems.TimeCompile.Seconds(), 'f', -1, 64))

	buf.WriteString(MCLargeLogRowPrefixStr + fmt.Sprintf("%v%v%v", MCLargeLogRewriteTimeStr,
		MCLargeLogSpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationRewrite.Seconds(), 'f', -1, 64)))
	buf.WriteString("\n")

	writeSlowLogItem(&buf, MCLargeLogOptimizeTimeStr, strconv.FormatFloat(logItems.TimeOptimize.Seconds(), 'f', -1, 64))

	if execDetailStr := logItems.ExecDetail.LargeLogString(); len(execDetailStr) > 0 {
		buf.WriteString(MCLargeLogRowPrefixStr + execDetailStr + "\n")
	}

	if len(s.CurrentDB) > 0 {
		writeSlowLogItem(&buf, MCLargeLogDBStr, strings.ToLower(s.CurrentDB))
	}

	if len(logItems.Digest) > 0 {
		writeSlowLogItem(&buf, MCLargeLogDigestStr, logItems.Digest)
	}
	if logItems.MemMax > 0 {
		writeSlowLogItem(&buf, MCLargeLogMemMax, strconv.FormatInt(logItems.MemMax, 10))
	}
	if logItems.DiskMax > 0 {
		writeSlowLogItem(&buf, MCLargeLogDiskMax, strconv.FormatInt(logItems.DiskMax, 10))
	}

	writeSlowLogItem(&buf, MCLargeLogResultRows, strconv.FormatInt(logItems.ResultRows, 10))
	writeSlowLogItem(&buf, MCLargeLogSuccStr, strconv.FormatBool(logItems.Succ))

	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, MCLargeLogPlan, logItems.Plan)
	}

	if s.CurrentDBChanged {
		buf.WriteString(fmt.Sprintf("use %s;\n", strings.ToLower(s.CurrentDB)))
		s.CurrentDBChanged = false
	}
	var b bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &b)
	gz := gzip.NewWriter(encoder)

	var err error
	if _, err = gz.Write([]byte(logItems.SQL)); err == nil {
		err = gz.Close()
	}

	if err != nil {
		return "", err
	}

	encoder.Close()

	buf.WriteString(MCLargeLogGzipPrefixStr)
	buf.Write(b.Bytes())
	buf.WriteString(MCLargeLogSQLSuffixStr)
	return buf.String(), nil
}
