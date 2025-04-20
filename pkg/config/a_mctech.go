// add by zhangbing

package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
	SQLChecker SQLChecker    `toml:"sql-checker" json:"sql-checker"`
	Metrics    MctechMetrics `toml:"metrics" json:"metrics"`
}

// SQLChecker switches to Enabled/Disabled fobidden sql query by digest
type SQLChecker struct {
	Enabled bool `toml:"enabled" json:"enabled"`
}

// MctechMetrics metrics record used
type MctechMetrics struct {
	Ignore MetricsIgnore `toml:"ignore" json:"ignore"` // 忽略的sql配置Ignore

	QueryLog   QueryLog   `toml:"query-log" json:"query-log"`
	LargeQuery LargeQuery `toml:"large-query" json:"large-query"`
	SQLTrace   SQLTrace   `toml:"sql-trace" json:"sql-trace"`
}

// SQLTrace full sql trace record used
type SQLTrace struct {
	Enabled           bool   `toml:"enabled" json:"enabled"`                       // 是否记录所有sql执行结果到独立文件中
	Filename          string `toml:"file-name" json:"file-name"`                   // 日志文件名称
	FileMaxDays       int    `toml:"file-max-days" json:"file-max-days"`           // 日志最长保存天数
	FileMaxSize       int    `toml:"file-max-size" json:"file-max-size"`           // 单个文件最大长度
	CompressThreshold int    `toml:"compress-threshold" json:"compress-threshold"` // 启用sql文本压缩的阈值
	FullSQLDir        string `toml:"full-sql-dir" json:"full-sql-dir"`             // sql重新导入数据库后，压缩的全量sql存储位置
	Group             string `toml:"group" json:"group"`                           // 默认的sql日志所属分组,一般是按照集群分组
}

// MetricsIgnore metrics ignore config
type MetricsIgnore struct {
	ByDatabases []string `toml:"by-databases" json:"by-databases"` // 需要排除记录的数据库
	ByRoles     []string `toml:"by-roles" json:"by-roles"`         // 属于给定角色列表中的sql不记录
}

// QueryLog sql log record used
type QueryLog struct {
	Enabled   bool `toml:"enabled" json:"enabled"`       // 是否启用日志里记录sql片断
	MaxLength int  `toml:"max-length" json:"max-length"` // 日志里记录的sql最大值
}

// LargeQuery large query log record used
type LargeQuery struct {
	Enabled     bool     `toml:"enabled" json:"enabled"`             // 是否启用large sql跟踪
	Filename    string   `toml:"file-name" json:"file-name"`         // 日志文件名称
	FileMaxDays int      `toml:"file-max-days" json:"file-max-days"` // 日志最长保存天数
	FileMaxSize int      `toml:"file-max-size" json:"file-max-size"` // 单个文件最大长度
	Threshold   int      `toml:"threshold" json:"threshold"`         // 超出该长度的sql会记录到数据库某个位置
	Types       []string `toml:"types" json:"types"`                 // 记录的sql类型
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
	Enabled    bool     `toml:"enabled" json:"enabled"`       // 是否开启同一sql语句中引用的数据库共存约束检查
	Compatible bool     `toml:"compatible" json:"compatible"` // 临时开关
	APIPrefix  string   `toml:"api-prefix" json:"api-prefix"` // 获取global_dw_*的当前索引的服务地址前缀
	Mutex      []string `toml:"mutex" json:"mutex"`           //
	Exclude    []string `toml:"exclude" json:"exclude"`       // 被排除在约束检查外的数据库名称
	Across     []string `toml:"across" json:"across"`         // 额外允许跨库查询的数据库对。每一项为'|'分隔的数据库名
	Excepts    []string `toml:"excepts" json:"excepts"`       // 排除在跨库约束检查之外的服务或依赖包列表。每一项的格式为: 1. {service}; 2. {service}.{product line}; 3. {package name}
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
	DbMatches []string `toml:"db-matches" json:"db-matches"` // 插入version的表所属的数据库名需要满足的条件
}

const (
	// DefaultSequenceMaxFetchCount one of config default value
	DefaultSequenceMaxFetchCount = 1000
	// DefaultSequenceBackend one of config default value
	DefaultSequenceBackend = 5

	// DefaultDbCheckerEnabled one of config default value
	DefaultDbCheckerEnabled = false
	// DefaultDbCheckerCompatible one of config default value
	DefaultDbCheckerCompatible = true

	// DefaultTenantEnabled one of config default value
	DefaultTenantEnabled = false
	// DefaultTenantForbiddenPrepare one of config default value
	DefaultTenantForbiddenPrepare = false

	// DefaultDDLVersionEnabled one of config default value
	DefaultDDLVersionEnabled = false
	// DefaultDDLVersionColumnName one of config default value
	DefaultDDLVersionColumnName = "__version"
	// DefaultMPPValue one of config default value
	DefaultMPPValue = "allow"

	// DefaultMetricsQueryLogEnabled one of config default value
	DefaultMetricsQueryLogEnabled = false
	// DefaultMetricsQueryLogMaxLength one of config default value
	DefaultMetricsQueryLogMaxLength = 4 * 1024 // 默认最大记录4K

	// DefaultMetricsLargeQueryEnabled one of config default value
	DefaultMetricsLargeQueryEnabled = false
	// DefaultMetricsLargeQueryFilename one of config default value
	DefaultMetricsLargeQueryFilename = "mctech_large_query_log.log"
	// DefaultMetricsLargeQueryFileMaxDays one of config default value
	DefaultMetricsLargeQueryFileMaxDays = 3
	// DefaultMetricsLargeQueryFileMaxSize one of config default value
	DefaultMetricsLargeQueryFileMaxSize = 512 // 512MB
	// DefaultMetricsLargeQueryThreshold one of config default value
	DefaultMetricsLargeQueryThreshold = 4 * 1024 * 1024

	// DefaultMetricsSQLTraceEnabled one of config default value
	DefaultMetricsSQLTraceEnabled = false
	// DefaultMetricsSQLTraceFilename one of config default value
	DefaultMetricsSQLTraceFilename = "mctech_tidb_full_sql.log"
	// DefaultMetricsSQLTraceCompressThreshold one of config default value
	DefaultMetricsSQLTraceCompressThreshold = 4 * 1024
	// DefaultMetricsSQLTraceFileMaxDays one of config default value
	DefaultMetricsSQLTraceFileMaxDays = 3
	// DefaultMetricsSQLTraceFileMaxSize one of config default value
	DefaultMetricsSQLTraceFileMaxSize = 512 // 512MB
	// DefaultMetricsSQLTraceFullSQLDir  one of config default value
	DefaultMetricsSQLTraceFullSQLDir = ""
	// DefaultMetricsSQLTraceGroup one of config default value
	DefaultMetricsSQLTraceGroup = ""
)

var (
	// DefaultDDLVersionDbMatches default value of config.DDL.Version.DbMatches
	DefaultDDLVersionDbMatches = []string{"global_*", "asset_*", "public_*", "*_custom"}

	// DefaultDbCheckerMutex default value of config.DbChecker.Mutex
	DefaultDbCheckerMutex = []string{"asset_*", "global_*"}
	// DefaultDbCheckerExcepts default value of config.DbChecker.Excepts
	DefaultDbCheckerExcepts = []string{}
	// DefaultDbCheckerExclude default value of config.DbChecker.Exclude
	DefaultDbCheckerExclude = []string{
		"global_platform",
		"global_ipm",
		"global_dw_*",
		"global_dwb",
	}
	// DefaultDbCheckerAcross default value of config.DbChecker.Mutex
	DefaultDbCheckerAcross = []string{"global_mtlp|global_ma"}

	// DefaultMetricsIgnoreByDatabases default value of config.Metrics.Ignore.Databases
	DefaultMetricsIgnoreByDatabases = []string{
		"test", "dp_stat",
		"mysql", "information_schema", "metrics_schema", "performance_schema",
	}
	// DefaultMetricsIgnoreByRoles default value of config.Metrics.Ignore.ByRoles
	DefaultMetricsIgnoreByRoles = []string{"mc_write", "mc_read", "sm_write", "sm_read"}

	// DefaultAllowMetricsLargeQueryTypes default value of config.Metrics.LargeQuery.Types
	DefaultAllowMetricsLargeQueryTypes = []string{"delete", "insert", "update", "select"}
)

func init() {
	defaultConf.MCTech = MCTech{
		Sequence: Sequence{
			Mock:          false,
			Debug:         false,
			MaxFetchCount: DefaultSequenceMaxFetchCount,
			Backend:       DefaultSequenceBackend,
			APIPrefix:     "http://node-infra-sequence-service.mc/",
		},
		Encryption: Encryption{
			Mock:      false,
			AccessID:  "oJEKJh1wvqncJYASxp1Iiw",
			APIPrefix: "http://node-infra-encryption-service.mc/",
		},
		DbChecker: DbChecker{
			Enabled:    DefaultDbCheckerEnabled,
			Compatible: DefaultDbCheckerCompatible,
			APIPrefix:  "http://node-infra-dim-service.mc/",
			Mutex:      []string{},
			Exclude:    []string{},
			Across:     []string{},
			Excepts:    []string{},
		},
		Tenant: Tenant{
			Enabled:          DefaultTenantEnabled,
			ForbiddenPrepare: DefaultTenantForbiddenPrepare,
		},
		DDL: DDL{
			Version: VersionColumn{
				Enabled:   DefaultDDLVersionEnabled,
				Name:      DefaultDDLVersionColumnName,
				DbMatches: []string{},
			},
		},
		MPP: MPP{
			DefaultValue: DefaultMPPValue,
		},
		Metrics: MctechMetrics{
			Ignore: MetricsIgnore{
				ByDatabases: DefaultMetricsIgnoreByDatabases,
				ByRoles:     DefaultMetricsIgnoreByRoles,
			},
			QueryLog: QueryLog{
				Enabled:   DefaultMetricsQueryLogEnabled,
				MaxLength: DefaultMetricsQueryLogMaxLength,
			},
			LargeQuery: LargeQuery{
				Enabled:     DefaultMetricsLargeQueryEnabled,
				Filename:    DefaultMetricsLargeQueryFilename,
				FileMaxDays: DefaultMetricsLargeQueryFileMaxDays,
				FileMaxSize: DefaultMetricsLargeQueryFileMaxSize,
				Threshold:   DefaultMetricsLargeQueryThreshold,
				Types:       DefaultAllowMetricsLargeQueryTypes,
			},
			SQLTrace: SQLTrace{
				Enabled:           DefaultMetricsSQLTraceEnabled,
				Filename:          DefaultMetricsSQLTraceFilename,
				FileMaxDays:       DefaultMetricsSQLTraceFileMaxDays,
				FileMaxSize:       DefaultMetricsSQLTraceFileMaxSize,
				CompressThreshold: DefaultMetricsSQLTraceCompressThreshold,
				FullSQLDir:        DefaultMetricsSQLTraceFullSQLDir,
				Group:             DefaultMetricsSQLTraceGroup,
			},
		},
		SQLChecker: SQLChecker{
			Enabled: false,
		},
	}
}

// ########################### Option ##########################################

var (
	mctechConf       atomic.Value
	mctechConfigLock sync.Mutex
)

// GetMCTechConfig get mctech option
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

		values := make(map[string]any)
		err = json.Unmarshal([]byte(val.(string)), &values)
		if err != nil {
			panic(err)
		}

		for k, v := range values {
			switch k {
			case "Sequence.Mock":
				opts.Sequence.Mock = toBool(v)
			case "DbChecker.Compatible":
				opts.DbChecker.Compatible = toBool(v)
			case "DbChecker.Across":
				opts.DbChecker.Across = toList(v)
			case "DbChecker.Excepts":
				opts.DbChecker.Excepts = toList(v)
			case "Encryption.Mock":
				opts.Encryption.Mock = toBool(v)
			case "Tenant.Enabled":
				opts.Tenant.Enabled = toBool(v)
			case "Tenant.ForbiddenPrepare":
				opts.Tenant.ForbiddenPrepare = toBool(v)
			case "DbChecker.Enabled":
				opts.DbChecker.Enabled = toBool(v)
			case "DDL.Version.Enabled":
				opts.DDL.Version.Enabled = toBool(v)
			case "Metrics.LargeQuery.Filename":
				opts.Metrics.LargeQuery.Filename = toString(v)
			case "Metrics.SqlTrace.FullSqlDir":
				opts.Metrics.SQLTrace.FullSQLDir = toString(v)
			case "Metrics.SqlTrace.Enabled":
				opts.Metrics.SQLTrace.Enabled = toBool(v)
			case "Metrics.SqlTrace.CompressThreshold":
				opts.Metrics.SQLTrace.CompressThreshold = toInt(v)
			case "Metrics.SqlTrace.Group":
				opts.Metrics.SQLTrace.Group = v.(string)
			case "SQLChecker.Enabled":
				opts.SQLChecker.Enabled = toBool(v)
			}
		}
		failpoint.Return(&opts)
	})
	return mctechOpts
}

func toBool(v any) bool {
	return v.(bool)
}

func toInt(v any) int {
	return int(v.(float64))
}

func toString(v any) string {
	return v.(string)
}

func toList(v any) []string {
	lst := v.([]any)
	strList := make([]string, 0, len(lst))
	for _, item := range lst {
		strList = append(strList, item.(string))
	}
	return strList
}

func storeMCTechConfig(config *Config) {
	mctechConfigLock.Lock()
	defer mctechConfigLock.Unlock()
	opts := &config.MCTech
	opts.Sequence.APIPrefix = formatURL(opts.Sequence.APIPrefix)
	opts.Encryption.APIPrefix = formatURL(opts.Encryption.APIPrefix)
	opts.DbChecker.APIPrefix = formatURL(opts.DbChecker.APIPrefix)

	opts.DDL.Version.DbMatches = DistinctSlice(append(opts.DDL.Version.DbMatches, DefaultDDLVersionDbMatches...))

	opts.DbChecker.Mutex = DistinctSlice(append(opts.DbChecker.Mutex, DefaultDbCheckerMutex...))
	opts.DbChecker.Exclude = DistinctSlice(append(opts.DbChecker.Exclude, DefaultDbCheckerExclude...))
	opts.DbChecker.Across = DistinctSlice(append(opts.DbChecker.Across, DefaultDbCheckerAcross...))
	opts.DbChecker.Excepts = DistinctSlice(append(opts.DbChecker.Excepts, DefaultDbCheckerExcepts...))

	opts.Metrics.Ignore.ByDatabases = DistinctSlice(append(opts.Metrics.Ignore.ByDatabases, DefaultMetricsIgnoreByDatabases...))
	opts.Metrics.Ignore.ByRoles = DistinctSlice(append(opts.Metrics.Ignore.ByRoles, DefaultMetricsIgnoreByRoles...))

	if opts.MPP.DefaultValue == "" {
		opts.MPP.DefaultValue = "allow"
	}

	sqlTrace := &opts.Metrics.SQLTrace
	if len(sqlTrace.Filename) == 0 {
		// 设置sqlTrace 日志文件的默认路径
		sqlTrace.Filename = DefaultMetricsSQLTraceFilename
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
	return apiPrefix
}

// StrToSlice convert string to slice. remove empty string
func StrToSlice(s string, sep string) []string {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return []string{}
	}

	parts := strings.Split(s, sep)
	nonEmptyParts := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) == 0 || slices.Contains(nonEmptyParts, part) {
			continue
		}
		nonEmptyParts = append(nonEmptyParts, part)
	}
	return nonEmptyParts
}

// DistinctSlice distinct slice. remove empty string
func DistinctSlice(s []string) []string {
	nonEmptyParts := make([]string, 0, len(s))
	for _, part := range s {
		part = strings.TrimSpace(part)
		if len(part) == 0 || slices.Contains(nonEmptyParts, part) {
			continue
		}
		nonEmptyParts = append(nonEmptyParts, part)
	}
	return nonEmptyParts
}

// StrToPossibleValueSlice convert string to slice. all item must be in possibleValues
func StrToPossibleValueSlice(s string, sep string, possibleValues []string) ([]string, string, bool) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return []string{}, "", true
	}
	parts := strings.Split(s, sep)

	var (
		result  = make([]string, 0, len(parts))
		illegal string
	)
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

var pattern = regexp.MustCompile("(?i){([^}]+)}")

func getHostName() string {
	failpoint.Inject("GetHostName", func() {
		failpoint.Return("tidb01")
	})

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

// GetRealLogFile get real log filename
func GetRealLogFile(filename string) (ret string, err error) {
	matches := pattern.FindStringSubmatch(filename)
	if matches == nil {
		return filename, nil
	}

	hostname := getHostName()
	defer func() {
		if r := recover(); r != nil {
			err, _ = r.(error)
		}
	}()
	realFileName := pattern.ReplaceAllStringFunc(filename, func(sub string) string {
		switch strings.ToLower(sub) {
		case "{hostname}":
			return hostname
		default:
			errMsg := fmt.Sprintf("metrics log filename template DO NOT support '%s' only allow '%s'。", matches[1], "hostname")
			logutil.BgLogger().Error(errMsg)
			panic(errors.New(errMsg))
		}
	})

	return realFileName, nil
}
