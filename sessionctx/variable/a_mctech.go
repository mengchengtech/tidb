package variable

import (
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
			GetGlobal: func(s *SessionVars) (string, error) {
				return config.GetMCTechConfig().MPP.DefaultValue, nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().MPP.DefaultValue = val
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeSqlEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsLargeSqlEnabled),
			GetGlobal: func(s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.LargeSql.Enabled), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.LargeSql.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeSqlTypes, skipInit: true, Type: TypeStr, Value: config.DefaultMetricsLargeSqlTypes,
			GetGlobal: func(s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().Metrics.LargeSql.SqlTypes, ","), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				items := strings.Split(val, ",")
				list := make([]string, len(items))
				for i, item := range items {
					item = strings.TrimSpace(item)
					if !slices.Contains(config.AllMetricsLargeSqlTypes, item) {
						panic(fmt.Errorf("sql types notsupported. %s", val))
					}
					list[i] = item
				}
				config.GetMCTechConfig().Metrics.LargeSql.SqlTypes = list
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsLargeSqlThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsLargeSqlThreshold),
			MinValue: 4 * 1024,
			GetGlobal: func(s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.LargeSql.Threshold), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.LargeSql.Threshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlLogEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultMetricsSqlLogEnabled),
			GetGlobal: func(s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().Metrics.SqlLog.Enabled), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().Metrics.SqlLog.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechMetricsSqlLogMaxLength, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultMetricsSqlLogMaxLength),
			MinValue: 16 * 1024,
			GetGlobal: func(s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().Metrics.SqlLog.MaxLength), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().Metrics.SqlLog.MaxLength = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultSqlTraceEnabled),
			GetGlobal: func(s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().SqlTrace.Enabled), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				config.GetMCTechConfig().SqlTrace.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceCompressThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSqlTraceCompressThreshold),
			MinValue: 16 * 1024,
			GetGlobal: func(s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().SqlTrace.CompressThreshold), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().SqlTrace.CompressThreshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceExcludeDbs, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultSqlTraceExcludeDbs, ","),
			GetGlobal: func(s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().SqlTrace.Exclude, ","), nil
			},
			SetGlobal: func(s *SessionVars, val string) error {
				items := strings.Split(val, ",")
				list := make([]string, len(items))
				for i, item := range items {
					list[i] = strings.TrimSpace(item)
				}
				config.GetMCTechConfig().SqlTrace.Exclude = list
				return nil
			},
		},
	}

	defaultSysVars = append(defaultSysVars, mctechSysVars...)
}

func LoadMctechSysVars() {
	option := config.GetMCTechConfig()
	SetSysVar(MCTechMPPDefaultValue, option.MPP.DefaultValue)

	SetSysVar(MCTechMetricsLargeSqlEnabled, BoolToOnOff(option.Metrics.LargeSql.Enabled))
	SetSysVar(MCTechMetricsLargeSqlTypes, strings.Join(option.Metrics.LargeSql.SqlTypes, ","))
	SetSysVar(MCTechMetricsLargeSqlThreshold, strconv.Itoa(option.Metrics.LargeSql.Threshold))
	SetSysVar(MCTechMetricsSqlLogEnabled, BoolToOnOff(option.Metrics.SqlLog.Enabled))
	SetSysVar(MCTechMetricsSqlLogMaxLength, strconv.Itoa(option.Metrics.SqlLog.MaxLength))

	SetSysVar(MCTechSqlTraceEnabled, BoolToOnOff(option.SqlTrace.Enabled))
	SetSysVar(MCTechSqlTraceCompressThreshold, strconv.Itoa(option.SqlTrace.CompressThreshold))
	SetSysVar(MCTechSqlTraceExcludeDbs, strings.Join(option.SqlTrace.Exclude, ","))
}
