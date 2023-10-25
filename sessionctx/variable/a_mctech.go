// add by zhangbing

package variable

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/execdetails"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	// MCTechLargeQueryRowPrefixStr is slow log row prefix.
	MCTechLargeQueryRowPrefixStr = "# "
	// MCTechLargeQuerySpaceMarkStr is large query log space mark.
	MCTechLargeQuerySpaceMarkStr = ": "
	// MCTechLargeQuerySQLSuffixStr is slow log suffix.
	MCTechLargeQuerySQLSuffixStr = ";"
	// MCTechLargeQueryGzipPrefixStr is compress sql prefix.
	MCTechLargeQueryGzipPrefixStr = "{gzip}"
	// MCTechLargeQueryStartPrefixStr is slow log start row prefix.
	MCTechLargeQueryStartPrefixStr = MCTechLargeQueryRowPrefixStr + MCTechLargeQueryTimeStr + MCTechLargeQuerySpaceMarkStr
	// MCTechLargeQueryUserAndHostStr is the user and host field name, which is compatible with MySQL.
	MCTechLargeQueryUserAndHostStr = "USER@HOST"

	// MCTechLargeQueryTimeStr is slow log field name.
	MCTechLargeQueryTimeStr = "TIME"
	// MCTechLargeQueryUserStr is slow log field name.
	MCTechLargeQueryUserStr = "USER"
	// MCTechLargeQueryHostStr only for slow_query table usage.
	MCTechLargeQueryHostStr = "HOST"
	// MCTechLargeQueryQueryTimeStr is slow log field name.
	MCTechLargeQueryQueryTimeStr = "QUERY_TIME"
	// MCTechLargeQueryParseTimeStr is the parse sql time.
	MCTechLargeQueryParseTimeStr = "PARSE_TIME"
	// MCTechLargeQueryCompileTimeStr is the compile plan time.
	MCTechLargeQueryCompileTimeStr = "COMPILE_TIME"
	// MCTechLargeQueryRewriteTimeStr is the rewrite time.
	MCTechLargeQueryRewriteTimeStr = "REWRITE_TIME"
	// MCTechLargeQueryOptimizeTimeStr is the optimization time.
	MCTechLargeQueryOptimizeTimeStr = "OPTIMIZE_TIME"

	// MCTechLargeQueryDBStr is slow log field name.
	MCTechLargeQueryDBStr = "DB"
	// MCTechLargeQuerySQLStr is slow log field name.
	MCTechLargeQuerySQLStr = "Query"
	// MCTechLargeQuerySuccStr is used to indicate whether this sql execute successfully.
	MCTechLargeQuerySuccStr = "SUCC"
	// MCTechLargeQueryMemMax is the max number bytes of memory used in this statement.
	MCTechLargeQueryMemMax = "MEM_MAX"
	// MCTechLargeQueryDiskMax is the nax number bytes of disk used in this statement.
	MCTechLargeQueryDiskMax = "DISK_MAX"
	// MCTechLargeQueryDigestStr is slow log field name.
	MCTechLargeQueryDigestStr = "DIGEST"
	// MCTechLargeQuerySQLLengthStr is large log length.
	MCTechLargeQuerySQLLengthStr = "SQL_LENGTH"
	// MCTechLargeQueryServiceStr is the service that large log maybe from.
	MCTechLargeQueryServiceStr = "SERVICE"
	// MCTechLargeQueryResultRows is the row count of the SQL result.
	MCTechLargeQueryResultRows = "RESULT_ROWS"
	// MCTechLargeQuerySQLTypeStr large sql type. (insert/update/delete/select......)
	MCTechLargeQuerySQLTypeStr = "SQL_TYPE"
	// MCTechLargeQueryPlan is used to record the query plan.
	MCTechLargeQueryPlan = "PLAN"
)

const (
	// MCTechSequenceMaxFetchCount is one of mctech config items
	MCTechSequenceMaxFetchCount = "mctech_sequence_max_fetch_count"
	// MCTechSequenceBackend is one of mctech config items
	MCTechSequenceBackend = "mctech_sequence_backend"

	// MCTechDbCheckerEnabled is one of mctech config items
	MCTechDbCheckerEnabled = "mctech_db_checker_enabled"
	// MCTechDbCheckerCompatible is one of mctech config items
	MCTechDbCheckerCompatible = "mctech_db_checker_compatible"

	// MCTechDbCheckerMutex is one of mctech config items
	MCTechDbCheckerMutex = "mctech_checker_mutex"
	// MCTechDbCheckerExclude is one of mctech config items
	MCTechDbCheckerExclude = "mctech_checker_exclude"
	// MCTechDbCheckerAcross is one of mctech config items
	MCTechDbCheckerAcross = "mctech_db_checker_across"

	// MCTechTenantEnabled is one of mctech config items
	MCTechTenantEnabled = "mctech_tenant_enabled"
	// MCTechTenantForbiddenPrepare is one of mctech config items
	MCTechTenantForbiddenPrepare = "mctech_tenant_forbidden_prepare"

	// MCTechDDLVersionEnabled is one of mctech config items
	MCTechDDLVersionEnabled = "mctech_ddl_version_enabled"
	// MCTechDDLVersionName is one of mctech config items
	MCTechDDLVersionName = "mctech_ddl_version_name"
	// MCTechDDLVersionDbMatches is one of mctech config items
	MCTechDDLVersionDbMatches = "mctech_ddl_version_db_matches"

	// MCTechMPPDefaultValue is one of mctech config items
	MCTechMPPDefaultValue = "mctech_mpp_default_value"

	// MCTechMetricsLargeQueryEnabled is one of mctech config items
	MCTechMetricsLargeQueryEnabled = "mctech_metrics_large_query_enabled"
	// MCTechMetricsLargeQueryFilename is one of mctech config items
	MCTechMetricsLargeQueryFilename = "mctech_metrics_large_query_file"
	// MCTechMetricsLargeQueryFileMaxDays is one of mctech config items
	MCTechMetricsLargeQueryFileMaxDays = "mctech_metrics_large_query_file_max_days"
	// MCTechMetricsLargeQueryFileMaxSize is one of mctech config items
	MCTechMetricsLargeQueryFileMaxSize = "mctech_metrics_large_query_file_max_size"
	// MCTechMetricsLargeQueryTypes is one of mctech config items
	MCTechMetricsLargeQueryTypes = "mctech_metrics_large_query_types"
	// MCTechMetricsLargeQueryThreshold is one of mctech config items
	MCTechMetricsLargeQueryThreshold = "mctech_metrics_large_query_threshold"

	// MCTechMetricsQueryLogEnabled is one of mctech config items
	MCTechMetricsQueryLogEnabled = "mctech_metrics_query_log_enabled"
	// MCTechMetricsQueryLogMaxLength is one of mctech config items
	MCTechMetricsQueryLogMaxLength = "mctech_metrics_query_log_max_length"

	// MCTechMetricsSQLTraceEnabled is one of mctech config items
	MCTechMetricsSQLTraceEnabled = "mctech_metrics_sql_trace_enabled"
	// MCTechMetricsSQLTraceFilename is one of mctech config items
	MCTechMetricsSQLTraceFilename = "mctech_metrics_sql_trace_file"
	// MCTechMetricsSQLTraceFileMaxSize is one of mctech config items
	MCTechMetricsSQLTraceFileMaxSize = "mctech_metrics_sql_trace_file_max_size"
	// MCTechMetricsSQLTraceFileMaxDays is one of mctech config items
	MCTechMetricsSQLTraceFileMaxDays = "mctech_metrics_sql_trace_file_max_days"
	// MCTechMetricsSQLTraceCompressThreshold is one of mctech config items
	MCTechMetricsSQLTraceCompressThreshold = "mctech_metrics_sql_trace_compress_threshold"
	// MCTechMetricsSqlTraceFullSqlDir is one of mctech config items
	MCTechMetricsSqlTraceFullSqlDir = "mctech_metrics_sql_trace_full_sql_dir"

	// MCTechMetricsExclude is one of mctech config items
	MCTechMetricsExclude = "mctech_metrics_exclude"
)

func init() {
	var mctechSysVars = []*SysVar{
		{Scope: ScopeNone, Name: MCTechSequenceMaxFetchCount, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSequenceMaxFetchCount)},
		{Scope: ScopeNone, Name: MCTechSequenceBackend, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSequenceBackend)},

		{Scope: ScopeNone, Name: MCTechDbCheckerEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultDbCheckerEnabled)},
		{Scope: ScopeGlobal, Name: MCTechDbCheckerCompatible, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultDbCheckerCompatible),
			GetGlobal: func(s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().DbChecker.Compatible), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().DbChecker.Compatible = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechDbCheckerMutex, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDbCheckerMutex, ",")},
		{Scope: ScopeNone, Name: MCTechDbCheckerExclude, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDbCheckerExclude, ",")},
		{Scope: ScopeNone, Name: MCTechDbCheckerAcross, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDbCheckerAcross, "|")},

		{Scope: ScopeNone, Name: MCTechTenantEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultTenantEnabled)},
		{Scope: ScopeNone, Name: MCTechTenantForbiddenPrepare, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultTenantForbiddenPrepare)},

		{Scope: ScopeNone, Name: MCTechDDLVersionEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultDDLVersionEnabled)},
		{Scope: ScopeNone, Name: MCTechDDLVersionName, skipInit: true, Type: TypeStr, Value: config.DefaultDDLVersionColumnName},
		{Scope: ScopeNone, Name: MCTechDDLVersionDbMatches, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultDDLVersionDbMatches, ",")},

		{Scope: ScopeGlobal, Name: MCTechMPPDefaultValue, skipInit: true, Type: TypeEnum, Value: config.DefaultMPPValue,
			PossibleValues: []string{"allow", "force", "disable"},
			GetGlobal: func(s *SessionVars) (string, error) {
				return config.GetMCTechConfig().MPP.DefaultValue, nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().MPP.DefaultValue = val
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsLargeQueryEnabled),
			GetGlobal: func(s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.LargeQuery.Enabled), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.LargeQuery.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechMetricsLargeQueryFilename, skipInit: true, Type: TypeBool, Value: config.DefaultMetricsLargeQueryFilename},
		{Scope: ScopeNone, Name: MCTechMetricsLargeQueryFileMaxDays, skipInit: true, Type: TypeBool, Value: strconv.Itoa(config.DefaultMetricsLargeQueryFileMaxDays)},
		{Scope: ScopeNone, Name: MCTechMetricsLargeQueryFileMaxSize, skipInit: true, Type: TypeBool, Value: strconv.Itoa(config.DefaultMetricsLargeQueryFileMaxSize)},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryTypes, skipInit: true, Type: TypeStr, Value: strings.Join(config.AllAllowMetricsLargeQueryTypes, ","),
			Validation: func(vars *SessionVars, _ string, original string, scope ScopeFlag) (string, error) {
				return validateEnumSet(original, ",", config.AllAllowMetricsLargeQueryTypes)
			},
			GetGlobal: func(s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().Metrics.LargeQuery.Types, ","), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.LargeQuery.Types = config.StrToSlice(val, ",")
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsLargeQueryThreshold),
			MinValue: 4 * 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.LargeQuery.Threshold), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.LargeQuery.Threshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsQueryLogEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsQueryLogEnabled),
			GetGlobal: func(s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.QueryLog.Enabled), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.QueryLog.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsQueryLogMaxLength, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsQueryLogMaxLength),
			MinValue: 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.QueryLog.MaxLength), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.QueryLog.MaxLength = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSQLTraceEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsSQLTraceEnabled),
			GetGlobal: func(s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.SQLTrace.Enabled), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.SQLTrace.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechMetricsSQLTraceFilename, skipInit: true, Type: TypeBool, Value: config.DefaultMetricsSQLTraceFilename},
		{Scope: ScopeNone, Name: MCTechMetricsSQLTraceFileMaxSize, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsSQLTraceFileMaxSize)},
		{Scope: ScopeNone, Name: MCTechMetricsSQLTraceFileMaxDays, skipInit: true, Type: TypeStr, Value: strconv.Itoa(config.DefaultMetricsSQLTraceFileMaxDays)},
		{Scope: ScopeGlobal, Name: MCTechMetricsSQLTraceCompressThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsSQLTraceCompressThreshold),
			MinValue: 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.SQLTrace.CompressThreshold), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}

				config.GetMCTechConfig().Metrics.SQLTrace.CompressThreshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsExclude, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultSQLTraceExclude, ","),
			GetGlobal: func(s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().Metrics.Exclude, ","), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.Exclude = config.StrToSlice(val, ",")
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlTraceFullSqlDir, skipInit: true, Type: TypeStr, Value: "",
			GetGlobal: func(s *SessionVars) (string, error) {
				return config.GetMCTechConfig().Metrics.SQLTrace.FullSqlDir, nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.SQLTrace.FullSqlDir = val
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

	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
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

// LoadMCTechSysVars init mctech custom global variables
func LoadMCTechSysVars() {
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
	SetSysVar(MCTechDbCheckerMutex, strings.Join(option.DbChecker.Mutex, ","))
	SetSysVar(MCTechDbCheckerExclude, strings.Join(option.DbChecker.Exclude, ","))
	SetSysVar(MCTechDbCheckerAcross, strings.Join(option.DbChecker.Across, ","))

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

	SetSysVar(MCTechMetricsSQLTraceEnabled, BoolToOnOff(option.Metrics.SQLTrace.Enabled))
	SetSysVar(MCTechMetricsSQLTraceFilename, option.Metrics.SQLTrace.Filename)
	SetSysVar(MCTechMetricsSQLTraceFileMaxSize, strconv.Itoa(option.Metrics.SQLTrace.FileMaxSize))
	SetSysVar(MCTechMetricsSQLTraceFileMaxDays, strconv.Itoa(option.Metrics.SQLTrace.FileMaxDays))
	SetSysVar(MCTechMetricsSQLTraceCompressThreshold, strconv.Itoa(option.Metrics.SQLTrace.CompressThreshold))

	SetSysVar(MCTechMetricsExclude, strings.Join(option.Metrics.Exclude, ","))
}

// MCTechLargeQueryLogItems is a collection of items that should be included in the
type MCTechLargeQueryLogItems struct {
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
	Service           string
	SQL               string
	SQLType           string
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
// # RESULT_ROWS: 1
// # SUCC: true
// # SQL_LENGTH: 26621
// # SERVICE: org-service
// # SQL_TYPE: insert
// # Plan: tidb_decode_plan('ZJAwCTMyXzcJMAkyMAlkYXRhOlRhYmxlU2Nhbl82CjEJMTBfNgkxAR0AdAEY1Dp0LCByYW5nZTpbLWluZiwraW5mXSwga2VlcCBvcmRlcjpmYWxzZSwgc3RhdHM6cHNldWRvCg==')
// use test;
// insert into t select * from t;

// LargeQueryFormat uses for formatting large query log.
func (s *SessionVars) LargeQueryFormat(logItems *MCTechLargeQueryLogItems) (string, error) {
	var buf bytes.Buffer

	if s.User != nil {
		hostAddress := s.User.Hostname
		if s.ConnectionInfo != nil {
			hostAddress = s.ConnectionInfo.ClientIP
		}
		writeSlowLogItem(&buf, MCTechLargeQueryUserAndHostStr, fmt.Sprintf("%s[%s] @ %s [%s]", s.User.Username, s.User.Username, s.User.Hostname, hostAddress))
	}
	writeSlowLogItem(&buf, MCTechLargeQueryQueryTimeStr, strconv.FormatFloat(logItems.TimeTotal.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, MCTechLargeQueryParseTimeStr, strconv.FormatFloat(logItems.TimeParse.Seconds(), 'f', -1, 64))
	writeSlowLogItem(&buf, MCTechLargeQueryCompileTimeStr, strconv.FormatFloat(logItems.TimeCompile.Seconds(), 'f', -1, 64))

	buf.WriteString(MCTechLargeQueryRowPrefixStr + fmt.Sprintf("%v%v%v", MCTechLargeQueryRewriteTimeStr,
		MCTechLargeQuerySpaceMarkStr, strconv.FormatFloat(logItems.RewriteInfo.DurationRewrite.Seconds(), 'f', -1, 64)))
	buf.WriteString("\n")

	writeSlowLogItem(&buf, MCTechLargeQueryOptimizeTimeStr, strconv.FormatFloat(logItems.TimeOptimize.Seconds(), 'f', -1, 64))

	if execDetailStr := logItems.ExecDetail.LargeQueryString(); len(execDetailStr) > 0 {
		buf.WriteString(MCTechLargeQueryRowPrefixStr + execDetailStr + "\n")
	}

	if len(s.CurrentDB) > 0 {
		writeSlowLogItem(&buf, MCTechLargeQueryDBStr, strings.ToLower(s.CurrentDB))
	}

	if len(logItems.Digest) > 0 {
		writeSlowLogItem(&buf, MCTechLargeQueryDigestStr, logItems.Digest)
	}
	if logItems.MemMax > 0 {
		writeSlowLogItem(&buf, MCTechLargeQueryMemMax, strconv.FormatInt(logItems.MemMax, 10))
	}
	if logItems.DiskMax > 0 {
		writeSlowLogItem(&buf, MCTechLargeQueryDiskMax, strconv.FormatInt(logItems.DiskMax, 10))
	}

	writeSlowLogItem(&buf, MCTechLargeQueryResultRows, strconv.FormatInt(logItems.ResultRows, 10))
	writeSlowLogItem(&buf, MCTechLargeQuerySuccStr, strconv.FormatBool(logItems.Succ))
	writeSlowLogItem(&buf, MCTechLargeQuerySQLLengthStr, strconv.Itoa(len(logItems.SQL)))
	writeSlowLogItem(&buf, MCTechLargeQuerySQLTypeStr, logItems.SQLType)
	if len(logItems.Service) > 0 {
		writeSlowLogItem(&buf, MCTechLargeQueryServiceStr, logItems.Service)
	}

	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, MCTechLargeQueryPlan, logItems.Plan)
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

	buf.WriteString(MCTechLargeQueryGzipPrefixStr)
	buf.Write(b.Bytes())
	buf.WriteString(MCTechLargeQuerySQLSuffixStr)
	return buf.String(), nil
}
