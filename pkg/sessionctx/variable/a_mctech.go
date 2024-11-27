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
	MCTechMPPDefaultValue            = "mctech_mpp_default_value"
	MCTechMetricsLargeQueryEnabled   = "mctech_metrics_large_query_enabled"
	MCTechMetricsLargeQueryTypes     = "mctech_metrics_large_query_types"
	MCTechMetricsLargeQueryThreshold = "mctech_metrics_large_query_threshold"
	MCTechMetricsQueryLogEnabled     = "mctech_metrics_query_log_enabled"
	MCTechMetricsQueryLogMaxLength   = "mctech_metrics_query_log_max_length"

	MCTechSqlTraceEnabled           = "mctech_sql_trace_enabled"
	MCTechSqlTraceCompressThreshold = "mctech_sql_trace_compress_threshold"
	MCTechSqlTraceExcludeDbs        = "mctech_sql_trace_exclude_dbs"
)

func init() {
	var mctechSysVars = []*SysVar{
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
		{Scope: ScopeGlobal, Name: MCTechSqlTraceEnabled, skipInit: true, Type: TypeBool, Value: BoolToOnOff(config.DefaultSqlTraceEnabled),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return BoolToOnOff(config.GetMCTechConfig().SqlTrace.Enabled), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				config.GetMCTechConfig().SqlTrace.Enabled = TiDBOptOn(val)
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceCompressThreshold, skipInit: true, Type: TypeInt, Value: strconv.Itoa(config.DefaultSqlTraceCompressThreshold),
			MinValue: 16 * 1024,
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strconv.Itoa(config.GetMCTechConfig().SqlTrace.CompressThreshold), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
				num, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				config.GetMCTechConfig().SqlTrace.CompressThreshold = num
				return nil
			},
		},
		{Scope: ScopeGlobal, Name: MCTechSqlTraceExcludeDbs, skipInit: true, Type: TypeStr, Value: strings.Join(config.DefaultSqlTraceExcludeDbs, ","),
			GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
				return strings.Join(config.GetMCTechConfig().SqlTrace.Exclude, ","), nil
			},
			SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
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

	SetSysVar(MCTechMetricsLargeQueryEnabled, BoolToOnOff(option.Metrics.LargeQuery.Enabled))
	SetSysVar(MCTechMetricsLargeQueryTypes, strings.Join(option.Metrics.LargeQuery.Types, ","))
	SetSysVar(MCTechMetricsLargeQueryThreshold, strconv.Itoa(option.Metrics.LargeQuery.Threshold))
	SetSysVar(MCTechMetricsQueryLogEnabled, BoolToOnOff(option.Metrics.QueryLog.Enabled))
	SetSysVar(MCTechMetricsQueryLogMaxLength, strconv.Itoa(option.Metrics.QueryLog.MaxLength))

	SetSysVar(MCTechSqlTraceEnabled, BoolToOnOff(option.SqlTrace.Enabled))
	SetSysVar(MCTechSqlTraceCompressThreshold, strconv.Itoa(option.SqlTrace.CompressThreshold))
	SetSysVar(MCTechSqlTraceExcludeDbs, strings.Join(option.SqlTrace.Exclude, ","))
}
