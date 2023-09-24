package variable

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/config"
	"golang.org/x/exp/slices"
)

const (
	MCTechMPPDefaultValue          = "mctech_mpp_default_value"
	MCTechMetricsLargeSqlEnabled   = "mctech_metrics_large_sql_enabled"
	MCTechMetricsLargeSqlTypes     = "mctech_metrics_large_sql_types"
	MCTechMetricsLargeSqlThreshold = "mctech_metrics_large_sql_threshold"
	MCTechMetricsSqlLogEnabled     = "mctech_metrics_sql_log_enabled"
	MCTechMetricsSqlLogMaxLength   = "mctech_metrics_sql_log_max_length"

	MCTechSqlTraceEnabled           = "mctech_sql_trace_enabled"
	MCTechSqlTraceCompressThreshold = "mctech_sql_trace_compress_threshold"
	MCTechSqlTraceExcludeDbs        = "mctech_sql_trace_exclude_dbs"
)

func init() {
	var mctechSysVars = []*SysVar{
		{Scope: ScopeGlobal, Name: MCTechMPPDefaultValue, skipInit: true, Type: TypeEnum, Value: config.DefaultMPPValue,
			PossibleValues: []string{"allow", "force", "disable"},
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return config.GetOption().MPPDefaultValue, nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetOption().MPPDefaultValue = val
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeSqlEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsLargeSqlEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetOption().MetricsLargeSqlEnabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetOption().MetricsLargeSqlEnabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeSqlTypes, skipInit: true, Type: TypeStr, Value: config.DefaultMetricsLargeSqlTypes,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strings.Join(config.GetOption().MetricsLargeSqlTypes, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				items := strings.Split(val, ",")
				list := make([]string, len(items))
				for i, item := range items {
					item = strings.TrimSpace(item)
					if !slices.Contains(config.AllMetricsLargeSqlTypes, item) {
						panic(fmt.Errorf("sql types notsupported. %s", val))
					}
					list[i] = item
				}
				config.GetOption().MetricsLargeSqlTypes = list
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeSqlThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsLargeSqlThreshold),
			MinValue: 4 * 1024,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetOption().MetricsLargeSqlThreshold), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetOption().MetricsLargeSqlThreshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlLogEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsSqlLogEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetOption().MetricsSqlLogEnabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetOption().MetricsSqlLogEnabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlLogMaxLength, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsSqlLogMaxLength),
			MinValue: 16 * 1024,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetOption().MetricsSqlLogMaxLength), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetOption().MetricsSqlLogMaxLength = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultSqlTraceEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetOption().SqlTraceEnabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetOption().SqlTraceEnabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceCompressThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSqlTraceCompressThreshold),
			MinValue: 16 * 1024,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetOption().SqlTraceCompressThreshold), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetOption().SqlTraceCompressThreshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceExcludeDbs, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultSqlTraceExcludeDbs, ","),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strings.Join(config.GetOption().SqlTraceExcludeDbs, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				items := strings.Split(val, ",")
				list := make([]string, len(items))
				for i, item := range items {
					list[i] = strings.TrimSpace(item)
				}
				config.GetOption().SqlTraceExcludeDbs = list
				return nil
			},
		},
	}

	defaultSysVars = append(defaultSysVars, mctechSysVars...)
}

func LoadMctechSysVars() {
	option := config.GetOption()
	SetSysVar(MCTechMPPDefaultValue, option.MPPDefaultValue)

	SetSysVar(MCTechMetricsLargeSqlEnabled, BoolToOnOff(option.MetricsLargeSqlEnabled))
	SetSysVar(MCTechMetricsLargeSqlTypes, strings.Join(option.MetricsLargeSqlTypes, ","))
	SetSysVar(MCTechMetricsLargeSqlThreshold, strconv.Itoa(option.MetricsLargeSqlThreshold))
	SetSysVar(MCTechMetricsSqlLogEnabled, BoolToOnOff(option.MetricsSqlLogEnabled))
	SetSysVar(MCTechMetricsSqlLogMaxLength, strconv.Itoa(option.MetricsSqlLogMaxLength))

	SetSysVar(MCTechSqlTraceEnabled, BoolToOnOff(option.SqlTraceEnabled))
	SetSysVar(MCTechSqlTraceCompressThreshold, strconv.Itoa(option.SqlTraceCompressThreshold))
	SetSysVar(MCTechSqlTraceExcludeDbs, strings.Join(option.SqlTraceExcludeDbs, ","))
}
