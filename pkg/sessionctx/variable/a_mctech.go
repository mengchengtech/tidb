// add by zhangbing

package variable

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
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

	MCTechMetricsLargeQueryEnabled   = "mctech_metrics_large_query_enabled"
	MCTechMetricsLargeQueryTypes     = "mctech_metrics_large_query_types"
	MCTechMetricsLargeQueryThreshold = "mctech_metrics_large_query_threshold"

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
		{Scope: ScopeNone, Name: MCTechSequenceMaxFetchCount, skipInit: true, Type: TypeInt, Value: "0"},
		{Scope: ScopeNone, Name: MCTechSequenceBackend, skipInit: true, Type: TypeInt, Value: "3"},

		{Scope: ScopeNone, Name: MCTechDbCheckerEnabled, skipInit: true, Type: TypeBool, Value: Off},

		{Scope: ScopeNone, Name: MCTechTenantEnabled, skipInit: true, Type: TypeBool, Value: Off},
		{Scope: ScopeNone, Name: MCTechTenantForbiddenPrepare, skipInit: true, Type: TypeBool, Value: Off},

		{Scope: ScopeNone, Name: MCTechDDLVersionEnabled, skipInit: true, Type: TypeBool, Value: Off},
		{Scope: ScopeNone, Name: MCTechDDLVersionName, skipInit: true, Type: TypeStr, Value: config.GetMCTechConfig().DDL.Version.Name},
		{Scope: ScopeNone, Name: MCTechDDLVersionDbMatches, skipInit: true, Type: TypeStr, Value: strings.Join(config.GetMCTechConfig().DDL.Version.DbMatches, ",")},

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
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryTypes, skipInit: true, Type: TypeStr, Value: config.DefaultMetricsLargeQueryTypes,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().Metrics.LargeQuery.Types, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				items := strings.Split(val, ",")
				list := make([]string, len(items))
				for i, item := range items {
					item = strings.TrimSpace(item)
					if !slices.Contains(config.AllMetricsLargeQueryTypes, item) {
						panic(fmt.Errorf("sql types notsupported. %s", val))
					}
					list[i] = item
				}
				config.GetMCTechConfig().Metrics.LargeQuery.Types = list
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeQueryThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsLargeQueryThreshold),
			MinValue: 4 * 1024,
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
			MinValue: 16 * 1024,
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
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlTraceEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultSqlTraceEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.SqlTrace.Enabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.SqlTrace.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeNone, Name: MCTechMetricsSqlTraceFilename, skipInit: true, Type: TypeBool, Value: ""},
		{Scope: ScopeNone, Name: MCTechMetricsSqlTraceFileMaxSize, skipInit: true, Type: TypeInt, Value: "0"},
		{Scope: ScopeNone, Name: MCTechMetricsSqlTraceFileMaxDays, skipInit: true, Type: TypeStr, Value: "0"},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlTraceCompressThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSqlTraceCompressThreshold),
			MinValue: 16 * 1024,
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
				items := strings.Split(val, ",")
				list := make([]string, len(items))
				for i, item := range items {
					list[i] = strings.TrimSpace(item)
				}
				config.GetMCTechConfig().Metrics.Exclude = list
				return nil
			},
		},
	}

	defaultSysVars = append(defaultSysVars, mctechSysVars...)
}

func LoadMctechSysVars() {
	option := config.GetMCTechConfig()
	SetSysVar(MCTechSequenceMaxFetchCount, strconv.FormatInt(option.Sequence.MaxFetchCount, 10))
	SetSysVar(MCTechSequenceBackend, strconv.FormatInt(option.Sequence.Backend, 10))

	SetSysVar(MCTechDbCheckerEnabled, BoolToOnOff(option.DbChecker.Enabled))

	SetSysVar(MCTechTenantEnabled, BoolToOnOff(option.Tenant.Enabled))
	SetSysVar(MCTechTenantForbiddenPrepare, BoolToOnOff(option.Tenant.ForbiddenPrepare))

	SetSysVar(MCTechDDLVersionEnabled, BoolToOnOff(option.DDL.Version.Enabled))
	SetSysVar(MCTechDDLVersionName, option.DDL.Version.Name)
	SetSysVar(MCTechDDLVersionDbMatches, strings.Join(option.DDL.Version.DbMatches, ","))

	SetSysVar(MCTechMPPDefaultValue, option.MPP.DefaultValue)

	SetSysVar(MCTechMetricsLargeQueryEnabled, BoolToOnOff(option.Metrics.LargeQuery.Enabled))
	SetSysVar(MCTechMetricsLargeQueryTypes, strings.Join(option.Metrics.LargeQuery.Types, ","))
	SetSysVar(MCTechMetricsLargeQueryThreshold, strconv.Itoa(option.Metrics.LargeQuery.Threshold))
	SetSysVar(MCTechMetricsQueryLogEnabled, BoolToOnOff(option.Metrics.QueryLog.Enabled))
	SetSysVar(MCTechMetricsQueryLogMaxLength, strconv.Itoa(option.Metrics.QueryLog.MaxLength))

	SetSysVar(MCTechMetricsSqlTraceEnabled, BoolToOnOff(option.Metrics.SqlTrace.Enabled))
	SetSysVar(MCTechMetricsSqlTraceFilename, option.Metrics.SqlTrace.Filename)

	SetSysVar(MCTechMetricsSqlTraceFileMaxSize, strconv.Itoa(option.Metrics.SqlTrace.FileMaxSize))
	SetSysVar(MCTechMetricsSqlTraceFileMaxDays, strconv.Itoa(option.Metrics.SqlTrace.FileMaxDays))

	SetSysVar(MCTechMetricsSqlTraceCompressThreshold, strconv.Itoa(option.Metrics.SqlTrace.CompressThreshold))
	SetSysVar(MCTechMetricsExcludeDbs, strings.Join(option.Metrics.Exclude, ","))
}
