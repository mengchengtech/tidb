// add by zhangbing

package config

import (
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

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
	SqlLog   SqlLog   `toml:"sql-log" json:"sql-log"`
	LargeSql LargeSql `toml:"large-sql" json:"large-sql"`
}

// SqlLog sql log record used
type SqlLog struct {
	Enabled   bool `toml:"enabled" json:"enabled"`       // 是否启用日志里记录sql片断
	MaxLength int  `toml:"max-length" json:"max-length"` // 日志里记录的sql最大值
}

type LargeSql struct {
	Enabled   bool     `toml:"enabled" json:"enabled"`     // 是否启用large sql跟踪
	Threshold int      `toml:"threshold" json:"threshold"` // 超出该长度的sql会记录到数据库某个位置
	SqlTypes  []string `toml:"sql-types" json:"sql-types"` // 记录的sql类型
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
	DefaultTenantEnabled             = false
	DefaultForbiddenPrepare          = false
	DefaultMPPValue                  = "allow"
	DefaultMetricsSqlLogEnabled      = false
	DefaultMetricsSqlLogMaxLength    = 16 * 1024 // 默认最大记录16K
	DefaultMetricsLargeSqlEnabled    = false
	DefaultMetricsLargeSqlThreshold  = 1 * 1024 * 1024
	DefaultSqlTraceEnabled           = false
	DefaultSqlTraceFileMaxDays       = 1
	DefaultSqlTraceFileMaxSize       = 1024 // 1024MB
	DefaultSqlTraceCompressThreshold = 16 * 1024
	DefaultMetricsLargeSqlTypes      = "delete,insert,update,select"
)

func initMCTechConfig() MCTech {
	return MCTech{
		Sequence: Sequence{
			Mock:          false,
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
			Enabled:          DefaultTenantEnabled,
			ForbiddenPrepare: DefaultForbiddenPrepare,
		},
		DDL: DDL{
			Version: VersionColumn{
				Enabled:   false,
				Name:      "__version",
				DbMatches: []string{},
			},
		},
		MPP: MPP{
			DefaultValue: DefaultMPPValue,
		},
		Metrics: MctechMetrics{
			SqlLog: SqlLog{
				Enabled:   DefaultMetricsSqlLogEnabled,
				MaxLength: DefaultMetricsSqlLogMaxLength,
			},
			LargeSql: LargeSql{
				Enabled:   DefaultMetricsLargeSqlEnabled,
				Threshold: DefaultMetricsLargeSqlThreshold,
				SqlTypes:  strings.Split(DefaultMetricsLargeSqlTypes, ","),
			},
		},
		SqlTrace: SqlTrace{
			Enabled:           DefaultSqlTraceEnabled,
			FileMaxDays:       DefaultSqlTraceFileMaxDays,
			FileMaxSize:       DefaultSqlTraceFileMaxSize,
			Exclude:           []string{},
			CompressThreshold: DefaultSqlTraceCompressThreshold,
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

// StoreMCTechConfig
func StoreMCTechConfig(opts *MCTech) {
	mctechConfigLock.Lock()
	defer mctechConfigLock.Unlock()

	opts.Sequence.APIPrefix = formatURL(opts.Sequence.APIPrefix)
	opts.Encryption.APIPrefix = formatURL(opts.Encryption.APIPrefix)
	opts.DbChecker.APIPrefix = formatURL(opts.DbChecker.APIPrefix)

	if len(opts.SqlTrace.Exclude) > 0 {
		opts.SqlTrace.Exclude = append(DefaultSqlTraceExcludeDbs, opts.SqlTrace.Exclude...)
	}

	if opts.MPP.DefaultValue == "" {
		opts.MPP.DefaultValue = "allow"
	}

	mctechConf.Store(opts)
}

var DefaultSqlTraceExcludeDbs = []string{
	"test", "dp_stat",
	"mysql", "information_schema", "metrics_schema", "performance_schema",
}

var AllMetricsLargeSqlTypes = strings.Split(DefaultMetricsLargeSqlTypes, ",")

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
