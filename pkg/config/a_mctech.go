// add by zhangbing

package config

import (
	"encoding/json"
	"net/url"
	"strings"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	SqlTrace   SqlTrace      `toml:"sql-trace" json:"sql-trace"`
}

type SqlTrace struct {
	Enabled           bool     `toml:"enabled" json:"enabled"`                       // 是否记录所有sql执行结果到独立文件中
	Filename          string   `toml:"file-name" json:"file-name"`                   // 日志文件名称
	FileMaxDays       int      `toml:"file-max-days" json:"file-max-days"`           // 日志最长保存天数
	FileMaxSize       int      `toml:"file-max-size" json:"file-max-size"`           // 单个文件最大长度
	Exclude           []string `toml:"exclude" json:"exclude"`                       // 需要排除记录的数据库，使用','分隔
	CompressThreshold int      `toml:"compress-threshold" json:"compress-threshold"` // 启用sql文本压缩的阈值
}

type MctechMetrics struct {
	QueryLog   QueryLog   `toml:"query-log" json:"query-log"`
	LargeQuery LargeQuery `toml:"large-query" json:"large-query"`
}

// QueryLog sql log record used
type QueryLog struct {
	Enabled   bool `toml:"enabled" json:"enabled"`       // 是否启用日志里记录sql片断
	MaxLength int  `toml:"max-length" json:"max-length"` // 日志里记录的sql最大值
}

type LargeQuery struct {
	Enabled   bool   `toml:"enabled" json:"enabled"`     // 是否启用large sql跟踪
	Threshold int    `toml:"threshold" json:"threshold"` // 超出该长度的sql会记录到数据库某个位置
	Types     string `toml:"types" json:"types"`         // 记录的sql类型
}

// Sequence mctech_sequence functions used
type Sequence struct {
	APIPrefix     string `toml:"api-prefix" json:"api-prefix"`
	Backend       int64  `toml:"backend" json:"backend"`
	Mock          bool   `toml:"mock" json:"mock"`
	Debug         bool   `toml:"debug" json:"debug"`
	MaxFetchCount int64  `toml:"max-fetch-count" json:"max-fetch-count"`
}

// DbChecker db isolation check used
type DbChecker struct {
	Enabled          bool     `toml:"enabled" json:"enabled"`
	APIPrefix        string   `toml:"api-prefix" json:"api-prefix"`
	MutexAcrossDbs   []string `toml:"mutex" json:"mutex"`
	ExcludeAcrossDbs []string `toml:"exclude" json:"exclude"`
	AcrossDbGroups   []string `toml:"across" json:"across"`
}

// Tenant append tenant condition used
type Tenant struct {
	Enabled          bool `toml:"enabled" json:"enabled"`
	ForbiddenPrepare bool `toml:"forbidden-prepare" json:"forbidden-prepare"`
}

// Encryption custom crypto function used
type Encryption struct {
	Mock      bool   `toml:"mock" json:"mock"`
	APIPrefix string `toml:"api-prefix" json:"api-prefix"`
	AccessID  string `toml:"access-id" json:"access-id"`
}

// DDL custom ddl config
type DDL struct {
	Version VersionColumn `toml:"version" json:"version"`
}

// MPP custom ddl config
type MPP struct {
	DefaultValue string `toml:"default-value" json:"default-value"`
}

// VersionColumn auto add version column
type VersionColumn struct {
	Enabled   bool     `toml:"enabled" json:"enabled"`
	Name      string   `toml:"name" json:"name"`
	DbMatches []string `toml:"db-matches" json:"db-matches"`
}

func init() {
	defaultConf.MCTech = MCTech{
		Sequence: Sequence{
			Mock:          true,
			Debug:         false,
			MaxFetchCount: 1000,
			Backend:       3,
			APIPrefix:     "http://node-infra-sequence-service.mc/",
		},
		Encryption: Encryption{
			Mock:      true,
			AccessID:  "oJEKJh1wvqncJYASxp1Iiw",
			APIPrefix: "http://node-infra-encryption-service.mc/",
		},
		DbChecker: DbChecker{
			Enabled:          false,
			APIPrefix:        "http://node-infra-dim-service.mc/",
			MutexAcrossDbs:   []string{},
			ExcludeAcrossDbs: []string{},
			AcrossDbGroups:   []string{},
		},
		Tenant: Tenant{
			Enabled:          false,
			ForbiddenPrepare: false,
		},
		DDL: DDL{
			Version: VersionColumn{
				Enabled: false,
				Name:    "__version",
			},
		},
		MPP: MPP{
			DefaultValue: "allow",
		},
		Metrics: MctechMetrics{
			QueryLog: QueryLog{
				Enabled:   false,
				MaxLength: 16 * 1024, // 默认最大记录16K
			},
			LargeQuery: LargeQuery{
				Enabled:   false,
				Threshold: 1 * 1024 * 1024,
				Types:     "delete,insert,update,select",
			},
		},
		SqlTrace: SqlTrace{
			Enabled:           false,
			FileMaxDays:       1,
			FileMaxSize:       1024,
			Exclude:           []string{},
			CompressThreshold: 16 * 1024,
		},
	}
}

// ########################### Option ##########################################

var defaultExcludeDbs = []string{
	"test", "dp_stat",
	"mysql", "information_schema", "metrics_schema", "performance_schema",
}

// Option mctech option
type Option struct {
	SequenceMock          bool   // sequence是否使用mock模式，不执行rpc调用，从本地返回固定的值
	SequenceDebug         bool   // 是否开启sequence取值过程的调试模式，输出更多的日志
	SequenceMaxFetchCount int64  // 每次rpc调用获取sequence的最大个数
	SequenceBackend       int64  // 后台并行获取sequence的最大并发数
	SequenceAPIPrefix     string // sequence服务的调用地址前缀

	MPPDefaultValue string // mpp 开关的默认值

	// encryption
	EncryptionMock      bool
	EncryptionAccessID  string
	EncryptionAPIPrefix string // encryption服务的调用地址前缀

	TenantEnabled          bool // 是否启用租户隔离
	TenantForbiddenPrepare bool // 禁用Prepare/Execute语句

	DbCheckerEnabled          bool // 是否开启同一sql语句中引用的数据库共存约束检查
	DbCheckerMutexAcrossDbs   []string
	DbCheckerExcludeAcrossDbs []string // 被排除在约束检查外的数据库名称
	DbCheckerAcrossDbGroups   []string
	DbCheckerAPIPrefix        string // 获取global_dw_*的当前索引的服务地址前缀

	DDLVersionColumnEnabled bool     // 是否开启 create table自动插入特定的version列的特性
	DDLVersionColumnName    string   // version列的名称
	DDLVersionFilters       []string // 插入version的表需要满足的条件

	MetricsLargeQueryEnabled   bool
	MetricsLargeQueryTypes     []string
	MetricsLargeQueryThreshold int
	MetricsQueryLogEnabled     bool
	MetricsQueryLogMaxLength   int

	SqlTraceEnabled           bool
	SqlTraceCompressThreshold int
	SqlTraceExcludeDbs        []string
}

var mctechOpts *Option

// GetOption get mctech option
func GetOption() *Option {
	if mctechOpts == nil {
		// 只能懒加载，需要在启动时先加载 config模块
		once := &sync.Once{}
		once.Do(initMCTechOption)
	}

	failpoint.Inject("GetMctechOption", func(val failpoint.Value) {
		opts := *mctechOpts
		values := make(map[string]bool)
		err := json.Unmarshal([]byte(val.(string)), &values)
		if err != nil {
			panic(err)
		}
		if v, ok := values["SequenceMock"]; ok {
			opts.SequenceMock = v
		}

		if v, ok := values["EncryptionMock"]; ok {
			opts.EncryptionMock = v
		}
		if v, ok := values["TenantEnabled"]; ok {
			opts.TenantEnabled = v
		}
		if v, ok := values["ForbiddenPrepare"]; ok {
			opts.TenantForbiddenPrepare = v
		}
		if v, ok := values["DbCheckerEnabled"]; ok {
			opts.DbCheckerEnabled = v
		}
		if v, ok := values["DDLVersionColumnEnabled"]; ok {
			opts.DDLVersionColumnEnabled = v
		}
		failpoint.Return(&opts)
	})
	return mctechOpts
}

func initMCTechOption() {
	if mctechOpts != nil {
		return
	}

	opts := GetGlobalConfig().MCTech
	option := &Option{
		SequenceMock:          opts.Sequence.Mock,
		SequenceDebug:         opts.Sequence.Debug,
		SequenceMaxFetchCount: opts.Sequence.MaxFetchCount,
		SequenceBackend:       opts.Sequence.Backend,
		SequenceAPIPrefix:     formatURL(opts.Sequence.APIPrefix),
		MPPDefaultValue:       opts.MPP.DefaultValue,

		EncryptionMock:      opts.Encryption.Mock,
		EncryptionAccessID:  opts.Encryption.AccessID,
		EncryptionAPIPrefix: formatURL(opts.Encryption.APIPrefix),

		TenantEnabled:             opts.Tenant.Enabled,
		TenantForbiddenPrepare:    opts.Tenant.ForbiddenPrepare,
		DbCheckerEnabled:          opts.DbChecker.Enabled,
		DbCheckerAPIPrefix:        formatURL(opts.DbChecker.APIPrefix),
		DbCheckerMutexAcrossDbs:   opts.DbChecker.MutexAcrossDbs,
		DbCheckerExcludeAcrossDbs: opts.DbChecker.ExcludeAcrossDbs,
		DbCheckerAcrossDbGroups:   opts.DbChecker.AcrossDbGroups,

		DDLVersionColumnEnabled: opts.DDL.Version.Enabled,
		DDLVersionColumnName:    opts.DDL.Version.Name,
		DDLVersionFilters:       opts.DDL.Version.DbMatches,

		MetricsLargeQueryEnabled:   opts.Metrics.LargeQuery.Enabled,
		MetricsLargeQueryThreshold: opts.Metrics.LargeQuery.Threshold,
		MetricsLargeQueryTypes:     strings.Split(strings.TrimSpace(opts.Metrics.LargeQuery.Types), ","),

		SqlTraceEnabled:           opts.SqlTrace.Enabled,
		SqlTraceCompressThreshold: opts.SqlTrace.CompressThreshold,
		SqlTraceExcludeDbs:        defaultExcludeDbs,

		MetricsQueryLogEnabled:   opts.Metrics.QueryLog.Enabled,
		MetricsQueryLogMaxLength: opts.Metrics.QueryLog.MaxLength,
	}

	if len(opts.SqlTrace.Exclude) > 0 {
		option.SqlTraceExcludeDbs = append(option.SqlTraceExcludeDbs, opts.SqlTrace.Exclude...)
	}

	if option.MPPDefaultValue == "" {
		option.MPPDefaultValue = "allow"
	}
	mctechOpts = option
}

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
