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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"go.uber.org/zap"
)

const (
	// MCLargeQueryRowPrefixStr is large query row prefix.
	MCLargeQueryRowPrefixStr = "# "
	// MCLargeQuerySpaceMarkStr is large query log space mark.
	MCLargeQuerySpaceMarkStr = ": "
	// MCLargeQuerySQLSuffixStr is large query suffix.
	MCLargeQuerySQLSuffixStr = ";"
	// MCLargeQueryGzipPrefixStr is compress sql prefix.
	MCLargeQueryGzipPrefixStr = "{gzip}"
	// MCLargeQueryStartPrefixStr is large query start row prefix.
	MCLargeQueryStartPrefixStr = MCLargeQueryRowPrefixStr + MCLargeQueryTimeStr + MCLargeQuerySpaceMarkStr
	// MCLargeQueryUserAndHostStr is the user and host field name, which is compatible with MySQL.
	MCLargeQueryUserAndHostStr = "USER@HOST"

	// MCLargeQueryTimeStr is large query field name.
	MCLargeQueryTimeStr = "TIME"
	// MCLargeQueryUserStr is large query field name.
	MCLargeQueryUserStr = "USER"
	// MCLargeQueryHostStr only for large query table usage.
	MCLargeQueryHostStr = "HOST"
	// MCLargeQueryQueryTimeStr is large query field name.
	MCLargeQueryQueryTimeStr = "QUERY_TIME"
	// MCLargeQueryParseTimeStr is the parse sql time.
	MCLargeQueryParseTimeStr = "PARSE_TIME"
	// MCLargeQueryCompileTimeStr is the compile plan time.
	MCLargeQueryCompileTimeStr = "COMPILE_TIME"
	// MCLargeQueryRewriteTimeStr is the rewrite time.
	MCLargeQueryRewriteTimeStr = "REWRITE_TIME"
	// MCLargeQueryOptimizeTimeStr is the optimization time.
	MCLargeQueryOptimizeTimeStr = "OPTIMIZE_TIME"

	// MCLargeQueryDBStr is large query field name.
	MCLargeQueryDBStr = "DB"
	// MCLargeQuerySQLStr is large query field name.
	MCLargeQuerySQLStr = "Query"
	// MCLargeQuerySuccStr is used to indicate whether this sql execute successfully.
	MCLargeQuerySuccStr = "SUCC"
	// MCLargeQueryMemMax is the max number bytes of memory used in this statement.
	MCLargeQueryMemMax = "MEM_MAX"
	// MCLargeQueryDiskMax is the nax number bytes of disk used in this statement.
	MCLargeQueryDiskMax = "DISK_MAX"
	// MCLargeQueryDigestStr is large query field name.
	MCLargeQueryDigestStr = "DIGEST"
	// MCLargeQuerySQLLengthStr is large query length.
	MCLargeQuerySQLLengthStr = "SQL_LENGTH"
	// MCLargeQueryAppNameStr is the service that large query maybe from.
	MCLargeQueryAppNameStr = "APP_NAME"
	// MCLargeQueryProductLineStr is the product-line that large query maybe from.
	MCLargeQueryProductLineStr = "PRODUCT_LINE"
	// MCLargeQueryPackageStr is the package that large query maybe from.
	MCLargeQueryPackageStr = "PACKAGE"
	// MCLargeQueryResultRows is the row count of the SQL result.
	MCLargeQueryResultRows = "RESULT_ROWS"
	// MCLargeQuerySQLTypeStr large sql type. (insert/update/delete/select......)
	MCLargeQuerySQLTypeStr = "SQL_TYPE"
	// MCLargeQueryPlan is used to record the query plan.
	MCLargeQueryPlan = "PLAN"
)

const (
	// MCTechDbCheckerCompatible is one of mctech config items
	MCTechDbCheckerCompatible = "mctech_db_checker_compatible"
	// MCTechDbCheckerExcepts is one of mctech config items
	MCTechDbCheckerExcepts = "mctech_db_checker_excepts"

	// MCTechMPPDefaultValue is one of mctech config items
	MCTechMPPDefaultValue = "mctech_mpp_default_value"

	// MCTechMetricsLargeQueryEnabled is one of mctech config items
	MCTechMetricsLargeQueryEnabled = "mctech_metrics_large_query_enabled"
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
	// MCTechMetricsSQLTraceCompressThreshold is one of mctech config items
	MCTechMetricsSQLTraceCompressThreshold = "mctech_metrics_sql_trace_compress_threshold"

	// MCTechMetricsIgnoreByRoles is one of mctech config items
	MCTechMetricsIgnoreByRoles = "mctech_metrics_ignore_by_roles"
	// MCTechMetricsIgnoreByDatabases is one of mctech config items
	MCTechMetricsIgnoreByDatabases = "mctech_metrics_ignore_by_databases"
)

var varMutex sync.Mutex

func atomicLoad[T any](pt *T) T {
	varMutex.Lock()
	defer varMutex.Unlock()
	return *pt
}

func atomicStore[T any](pt *T, v T) {
	varMutex.Lock()
	defer varMutex.Unlock()
	*pt = v
}

func init() {
	var mctechSysVars = []*SysVar{
		{Scope: ScopeGlobal, Name: MCTechDbCheckerCompatible, Type: TypeBool, Value: BoolToOnOff(config.DefaultDbCheckerCompatible),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(atomicLoad(&config.GetMCTechConfig().DbChecker.Compatible)), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				atomicStore(&config.GetMCTechConfig().DbChecker.Compatible, TiDBOptOn(val))
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMPPDefaultValue, Type: TypeEnum, Value: config.DefaultMPPValue,
			PossibleValues: []string{"allow", "force", "disable"},
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().MPP.DefaultValue)
				return v, nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := val
				atomicStore(&config.GetMCTechConfig().MPP.DefaultValue, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryEnabled, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsLargeQueryEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.LargeQuery.Enabled)
				return BoolToOnOff(v), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := TiDBOptOn(val)
				atomicStore(&config.GetMCTechConfig().Metrics.LargeQuery.Enabled, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryTypes, Type: TypeStr, Value: strings.Join(config.DefaultAllowMetricsLargeQueryTypes, ","),
			Validation: func(vars *SessionVars, _ string, original string, scope ScopeFlag) (string, error) {
				return validateEnumSet(original, ",", config.DefaultAllowMetricsLargeQueryTypes)
			},
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.LargeQuery.Types)
				return strings.Join(v, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := config.StrToSlice(val, ",")
				atomicStore(&config.GetMCTechConfig().Metrics.LargeQuery.Types, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryThreshold, Type: TypeUnsigned, Value: strconv.Itoa(config.DefaultMetricsLargeQueryThreshold),
			MinValue: 4 * 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.LargeQuery.Threshold)
				return strconv.Itoa(v), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := TidbOptInt(val, 0)
				atomicStore(&config.GetMCTechConfig().Metrics.LargeQuery.Threshold, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsQueryLogEnabled, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsQueryLogEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.QueryLog.Enabled)
				return BoolToOnOff(v), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := TiDBOptOn(val)
				atomicStore(&config.GetMCTechConfig().Metrics.QueryLog.Enabled, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsQueryLogMaxLength, Type: TypeUnsigned, Value: strconv.Itoa(config.DefaultMetricsQueryLogMaxLength),
			MinValue: 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.QueryLog.MaxLength)
				return strconv.Itoa(v), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := TidbOptInt(val, 0)
				atomicStore(&config.GetMCTechConfig().Metrics.QueryLog.MaxLength, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechDbCheckerExcepts, Type: TypeStr, Value: strings.Join(config.DefaultDbCheckerExcepts, ","),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().DbChecker.Excepts)
				return strings.Join(v, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := config.StrToSlice(val, ",")
				atomicStore(&config.GetMCTechConfig().DbChecker.Excepts, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSQLTraceEnabled, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsSQLTraceEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.SQLTrace.Enabled)
				return BoolToOnOff(v), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := TiDBOptOn(val)
				atomicStore(&config.GetMCTechConfig().Metrics.SQLTrace.Enabled, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSQLTraceCompressThreshold, Type: TypeUnsigned, Value: strconv.Itoa(config.DefaultMetricsSQLTraceCompressThreshold),
			MinValue: 1024, MaxValue: math.MaxInt64,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.SQLTrace.CompressThreshold)
				return strconv.Itoa(v), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := TidbOptInt(val, 0)
				atomicStore(&config.GetMCTechConfig().Metrics.SQLTrace.CompressThreshold, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsIgnoreByDatabases, Type: TypeStr, Value: strings.Join(config.DefaultMetricsIgnoreByDatabases, ","),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.Ignore.ByDatabases)
				return strings.Join(v, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := config.StrToSlice(val, ",")
				atomicStore(&config.GetMCTechConfig().Metrics.Ignore.ByDatabases, v)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsIgnoreByRoles, Type: TypeStr, Value: strings.Join(config.DefaultMetricsIgnoreByRoles, ","),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				v := atomicLoad(&config.GetMCTechConfig().Metrics.Ignore.ByRoles)
				return strings.Join(v, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				v := config.StrToSlice(val, ",")
				atomicStore(&config.GetMCTechConfig().Metrics.Ignore.ByRoles, v)
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

	SetSysVar(MCTechDbCheckerCompatible, BoolToOnOff(option.DbChecker.Compatible))
	SetSysVar(MCTechDbCheckerExcepts, strings.Join(option.DbChecker.Excepts, ","))

	SetSysVar(MCTechMPPDefaultValue, option.MPP.DefaultValue)

	SetSysVar(MCTechMetricsQueryLogEnabled, BoolToOnOff(option.Metrics.QueryLog.Enabled))
	SetSysVar(MCTechMetricsQueryLogMaxLength, strconv.Itoa(option.Metrics.QueryLog.MaxLength))

	SetSysVar(MCTechMetricsLargeQueryEnabled, BoolToOnOff(option.Metrics.LargeQuery.Enabled))
	SetSysVar(MCTechMetricsLargeQueryTypes, strings.Join(option.Metrics.LargeQuery.Types, ","))
	SetSysVar(MCTechMetricsLargeQueryThreshold, strconv.Itoa(option.Metrics.LargeQuery.Threshold))

	SetSysVar(MCTechMetricsSQLTraceEnabled, BoolToOnOff(option.Metrics.SQLTrace.Enabled))
	SetSysVar(MCTechMetricsSQLTraceCompressThreshold, strconv.Itoa(option.Metrics.SQLTrace.CompressThreshold))

	SetSysVar(MCTechMetricsIgnoreByRoles, strings.Join(option.Metrics.Ignore.ByRoles, ","))
	SetSysVar(MCTechMetricsIgnoreByDatabases, strings.Join(option.Metrics.Ignore.ByDatabases, ","))
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
	AppName           string
	ProductLine       string
	Package           string
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
// # APP_NAME: org-service
// # PRODUCT_LINE: pf
// # PACKAGE: @mctech/dp-impala
// # SQL_TYPE: insert
// # Plan: tidb_decode_plan('ZJAwCTMyXzcJMAkyMAlkYXRhOlRhYmxlU2Nhbl82CjEJMTBfNgkxAR0AdAEY1Dp0LCByYW5nZTpbLWluZiwraW5mXSwga2VlcCBvcmRlcjpmYWxzZSwgc3RhdHM6cHNldWRvCg==')
// use test;
// insert into t select * from t;

// LargeQueryFormat uses for formatting large query log.
func (s *SessionVars) LargeQueryFormat(logItems *MCLargeQueryItems) (string, error) {
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
	writeSlowLogItem(&buf, MCLargeQuerySQLLengthStr, strconv.Itoa(len(logItems.SQL)))
	writeSlowLogItem(&buf, MCLargeQuerySQLTypeStr, logItems.SQLType)
	if len(logItems.AppName) > 0 {
		writeSlowLogItem(&buf, MCLargeQueryAppNameStr, logItems.AppName)
	}
	if len(logItems.ProductLine) > 0 {
		writeSlowLogItem(&buf, MCLargeQueryProductLineStr, logItems.ProductLine)
	}
	if len(logItems.Package) > 0 {
		writeSlowLogItem(&buf, MCLargeQueryPackageStr, logItems.Package)
	}

	if len(logItems.Plan) != 0 {
		writeSlowLogItem(&buf, MCLargeQueryPlan, logItems.Plan)
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

	buf.WriteString(MCLargeQueryGzipPrefixStr)
	buf.Write(b.Bytes())
	buf.WriteString(MCLargeQuerySQLSuffixStr)
	return buf.String(), nil
}
