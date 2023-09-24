// add by zhangbing

package config

import (
	"encoding/json"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// MCTech mctech custom config
type MCTech struct {
	Sequence   Sequence      `toml:"sequence" json:"sequence"`
	Encryption Encryption    `toml:"encryption" json:"encryption"`
	DbChecker  DbChecker     `toml:"db-checker" json:"db-checker"`
	Tenant     Tenant        `toml:"tenant" json:"tenant"`
	DDL        DDL           `toml:"ddl" json:"ddl"`
	MPP        MPP           `toml:"mpp" json:"mpp"`
	Metrics    MctechMetrics `toml:"metrics" json:"metrics"`
}

type MctechMetrics struct {
	Exclude    []string   `toml:"exclude" json:"exclude"` // 需要排除记录的数据库，使用','分隔
	SqlLog     SqlLog     `toml:"sql-log" json:"sql-log"`
	LargeQuery LargeQuery `toml:"large-query" json:"large-query"`
	SqlTrace   SqlTrace   `toml:"sql-trace" json:"sql-trace"`
}

type SqlTrace struct {
	Enabled           bool   `toml:"enabled" json:"enabled"`                       // 是否记录所有sql执行结果到独立文件中
	Filename          string `toml:"file-name" json:"file-name"`                   // 日志文件名称
	FileMaxDays       int    `toml:"file-max-days" json:"file-max-days"`           // 日志最长保存天数
	FileMaxSize       int    `toml:"file-max-size" json:"file-max-size"`           // 单个文件最大长度
	CompressThreshold int    `toml:"compress-threshold" json:"compress-threshold"` // 启用sql文本压缩的阈值
}

// SqlLog sql log record used
type SqlLog struct {
	Enabled   bool `toml:"enabled" json:"enabled"`       // 是否启用日志里记录sql片断
	MaxLength int  `toml:"max-length" json:"max-length"` // 日志里记录的sql最大值
}

type LargeQuery struct {
	Enabled     bool     `toml:"enabled" json:"enabled"`             // 是否启用large sql跟踪
	Filename    string   `toml:"file-name" json:"file-name"`         // 日志文件名称
	FileMaxDays int      `toml:"file-max-days" json:"file-max-days"` // 日志最长保存天数
	FileMaxSize int      `toml:"file-max-size" json:"file-max-size"` // 单个文件最大长度
	Threshold   int      `toml:"threshold" json:"threshold"`         // 超出该长度的sql会记录到数据库某个位置
	SqlTypes    []string `toml:"sql-types" json:"sql-types"`         // 记录的sql类型
}

// Sequence mctech_sequence functions used
type Sequence struct {
	APIPrefix     string `toml:"api-prefix" json:"api-prefix"`           // sequence服务的调用地址前缀
	Backend       int64  `toml:"backend" json:"backend"`                 // 后台并行获取sequence的最大并发数
	Mock          bool   `toml:"mock" json:"mock"`                       // sequence是否使用mock模式，不执行rpc调用，从本地返回固定的值
	Debug         bool   `toml:"debug" json:"debug"`                     // 是否开启sequence取值过程的调试模式，输出更多的日志
	MaxFetchCount int64  `toml:"max-fetch-count" json:"max-fetch-count"` // 每次rpc调用获取sequence的最大个数
}

// DbChecker db isolation check used
type DbChecker struct {
	Enabled          bool     `toml:"enabled" json:"enabled"`       // 是否开启同一sql语句中引用的数据库共存约束检查
	APIPrefix        string   `toml:"api-prefix" json:"api-prefix"` // 获取global_dw_*的当前索引的服务地址前缀
	MutexAcrossDbs   []string `toml:"mutex" json:"mutex"`           //
	ExcludeAcrossDbs []string `toml:"exclude" json:"exclude"`       // 被排除在约束检查外的数据库名称
	AcrossDbGroups   []string `toml:"across" json:"across"`
}

// Tenant append tenant condition used
type Tenant struct {
	Enabled          bool `toml:"enabled" json:"enabled"`                     // 是否启用租户隔离
	ForbiddenPrepare bool `toml:"forbidden-prepare" json:"forbidden-prepare"` // 禁用Prepare/Execute语句
}

// Encryption custom crypto function used
type Encryption struct {
	Mock      bool   `toml:"mock" json:"mock"`             // 加密/解密是否使用mock模式，不执行rpc调用，从本地返回固定的值
	APIPrefix string `toml:"api-prefix" json:"api-prefix"` // encryption服务的调用地址前缀
	AccessID  string `toml:"access-id" json:"access-id"`   // 获取密钥接口使用的accessId
}

// DDL custom ddl config
type DDL struct {
	Version VersionColumn `toml:"version" json:"version"`
}

// MPP custom ddl config
type MPP struct {
	DefaultValue string `toml:"default-value" json:"default-value"` // mpp 开关的默认值
}

// VersionColumn auto add version column
type VersionColumn struct {
	Enabled   bool     `toml:"enabled" json:"enabled"`       // 是否开启 create table自动插入特定的version列的特性
	Name      string   `toml:"name" json:"name"`             // version列的名称
	DbMatches []string `toml:"db-matches" json:"db-matches"` // 插入version的表需要满足的条件
}

const (
	DefaultSequenceMaxFetchCount = 1000
	DefaultSequenceBackend       = 3

	DefaultDbCheckerEnabled = false

	DefaultTenantEnabled          = false
	DefaultTenantForbiddenPrepare = false

	DefaultDDLVersionEnabled    = false
	DefaultDDLVersionColumnName = "__version"
	DefaultDDLVersionDbMatches  = ""

	DefaultMPPValue = "allow"

	DefaultMetricsSqlLogEnabled   = false
	DefaultMetricsSqlLogMaxLength = 32 * 1024 // 默认最大记录32K

	DefaultMetricsLargeQueryEnabled     = false
	DefaultMetricsLargeQueryFilename    = "mctech_large_query_log.log"
	DefaultMetricsLargeQueryFileMaxDays = 1
	DefaultMetricsLargeQueryFileMaxSize = 1 * 1024 * 1024
	DefaultMetricsLargeQueryThreshold   = 1 * 1024 * 1024
	DefaultMetricsLargeQueryTypes       = "delete,insert,update,select"

	DefaultMetricsSqlTraceEnabled           = false
	DefaultMetricsSqlTraceFilename          = "mctech_tidb_full_sql.log"
	DefaultMetricsSqlTraceCompressThreshold = 16 * 1024
	DefaultMetricsSqlTraceFileMaxDays       = 1
	DefaultMetricsSqlTraceFileMaxSize       = 1024 // 1024MB
)

func newMCTech() MCTech {
	return MCTech{
		Sequence: Sequence{
			Mock:          false,
			Debug:         false,
			MaxFetchCount: DefaultSequenceMaxFetchCount,
			Backend:       DefaultSequenceBackend,
			APIPrefix:     "http://node-infra-sequence-service.mc/",
		},
		Encryption: Encryption{
			Mock:      true,
			AccessID:  "oJEKJh1wvqncJYASxp1Iiw",
			APIPrefix: "http://node-infra-encryption-service.mc/",
		},
		DbChecker: DbChecker{
			Enabled:          DefaultDbCheckerEnabled,
			APIPrefix:        "http://node-infra-dim-service.mc/",
			MutexAcrossDbs:   []string{},
			ExcludeAcrossDbs: []string{},
			AcrossDbGroups:   []string{},
		},
		Tenant: Tenant{
			Enabled:          DefaultTenantEnabled,
			ForbiddenPrepare: DefaultTenantForbiddenPrepare,
		},
		DDL: DDL{
			Version: VersionColumn{
				Enabled:   DefaultDDLVersionEnabled,
				Name:      DefaultDDLVersionColumnName,
				DbMatches: StrToSlice(DefaultDDLVersionDbMatches, ","),
			},
		},
		MPP: MPP{
			DefaultValue: DefaultMPPValue,
		},
		Metrics: MctechMetrics{
			Exclude: []string{},
			SqlLog: SqlLog{
				Enabled:   DefaultMetricsSqlLogEnabled,
				MaxLength: DefaultMetricsSqlLogMaxLength,
			},
			LargeQuery: LargeQuery{
				Enabled:     DefaultMetricsLargeQueryEnabled,
				Filename:    DefaultMetricsLargeQueryFilename,
				FileMaxDays: DefaultMetricsLargeQueryFileMaxDays,
				FileMaxSize: DefaultMetricsLargeQueryFileMaxSize,
				Threshold:   DefaultMetricsLargeQueryThreshold,
				SqlTypes:    StrToSlice(DefaultMetricsLargeQueryTypes, ","),
			},
			SqlTrace: SqlTrace{
				Enabled:           DefaultMetricsSqlTraceEnabled,
				Filename:          DefaultMetricsSqlTraceFilename,
				FileMaxDays:       DefaultMetricsSqlTraceFileMaxDays,
				FileMaxSize:       DefaultMetricsSqlTraceFileMaxSize,
				CompressThreshold: DefaultMetricsSqlTraceCompressThreshold,
			},
		},
	}
}

// ########################### Option ##########################################

var (
	mctechConf       atomic.Value
	mctechConfigLock sync.Mutex
)

// GetOption get mctech option
func GetMCTechConfig() *MCTech {
	mctechOpts := mctechConf.Load().(*MCTech)

	failpoint.Inject("GetMCTechConfig", func(val failpoint.Value) {
		bytes, err := json.Marshal(mctechOpts)
		if err != nil {
			panic(err)
		}
		// 深拷贝MCTech对象
		var opts MCTech
		err = json.Unmarshal(bytes, &opts)
		if err != nil {
			panic(err)
		}

		values := make(map[string]bool)
		err = json.Unmarshal([]byte(val.(string)), &values)
		if err != nil {
			panic(err)
		}
		if v, ok := values["Sequence.Mock"]; ok {
			opts.Sequence.Mock = v
		}

		if v, ok := values["Encryption.Mock"]; ok {
			opts.Encryption.Mock = v
		}
		if v, ok := values["Tenant.Enabled"]; ok {
			opts.Tenant.Enabled = v
		}
		if v, ok := values["Tenant.ForbiddenPrepare"]; ok {
			opts.Tenant.ForbiddenPrepare = v
		}
		if v, ok := values["DbChecker.Enabled"]; ok {
			opts.DbChecker.Enabled = v
		}
		if v, ok := values["DDL.Version.Enabled"]; ok {
			opts.DDL.Version.Enabled = v
		}
		failpoint.Return(&opts)
	})
	return mctechOpts
}

func storeMCTechConfig(config *Config) {
	mctechConfigLock.Lock()
	defer mctechConfigLock.Unlock()
	opts := &config.MCTech
	opts.Sequence.APIPrefix = formatURL(opts.Sequence.APIPrefix)
	opts.Encryption.APIPrefix = formatURL(opts.Encryption.APIPrefix)
	opts.DbChecker.APIPrefix = formatURL(opts.DbChecker.APIPrefix)

	if len(opts.Metrics.Exclude) > 0 {
		opts.Metrics.Exclude = append(DefaultSqlTraceExcludeDbs, opts.Metrics.Exclude...)
	} else {
		opts.Metrics.Exclude = DefaultSqlTraceExcludeDbs
	}

	if opts.MPP.DefaultValue == "" {
		opts.MPP.DefaultValue = "allow"
	}

	sqlTrace := &opts.Metrics.SqlTrace
	if len(sqlTrace.Filename) == 0 {
		// 设置sqlTrace 日志文件的默认路径
		sqlTrace.Filename = DefaultMetricsSqlTraceFilename
	}

	largeQuery := &opts.Metrics.LargeQuery
	if len(largeQuery.Filename) == 0 {
		// 设置large query 日志文件的路径
		sqlTrace.Filename = DefaultMetricsLargeQueryFilename
	}

	// 当前方法会多次运行，在第一次运行时 config.Log.File.Filename 不一定有值。
	// 需要保证在多次运行的情况下路径设置的正确性
	logFile := config.Log.File.Filename
	var logDir string
	if len(logFile) > 0 {
		logDir = filepath.Dir(logFile)
	}

	if len(logDir) > 0 {
		if !filepath.IsAbs(sqlTrace.Filename) {
			sqlTrace.Filename = filepath.Join(logDir, sqlTrace.Filename)
		}

		if !filepath.IsAbs(largeQuery.Filename) {
			largeQuery.Filename = filepath.Join(logDir, largeQuery.Filename)
		}
	}

	mctechConf.Store(opts)
}

var DefaultSqlTraceExcludeDbs = []string{
	"test", "dp_stat",
	"mysql", "information_schema", "metrics_schema", "performance_schema",
}

var AllMetricsLargeQueryTypes = strings.Split(DefaultMetricsLargeQueryTypes, ",")

func formatURL(str string) string {
	u, err := url.Parse(str)
	if err != nil {
		log.Error("apiPrefix format error.", zap.String("apiPrefix", str), zap.Error(err))
		panic(err)
	}

	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	apiPrefix := u.String()

	log.Info("api prefix: " + apiPrefix)
	return apiPrefix
}

// StrToSlice
func StrToSlice(s string, sep string) []string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return []string{}
	}

	parts := strings.Split(s, sep)
	var nonEmptyParts []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) == 0 || slices.Contains(nonEmptyParts, part) {
			continue
		}
		nonEmptyParts = append(nonEmptyParts, part)
	}
	return nonEmptyParts
}

// StrToPossibleValueSlice
func StrToPossibleValueSlice(s string, sep string, possibleValues []string) ([]string, string, bool) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return []string{}, "", true
	}

	var (
		result  []string
		illegal string
	)
	parts := strings.Split(s, sep)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) == 0 || slices.Contains(result, part) {
			continue
		}
		if !slices.Contains(possibleValues, part) {
			illegal = part
			return result, illegal, false
		}

		result = append(result, part)
	}
	return result, "", true
}
