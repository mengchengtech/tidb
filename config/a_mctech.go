// add by zhangbing

package config

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
	Enabled           bool   `toml:"enabled" json:"enabled"`                       // 是否记录所有sql执行结果到独立文件中
	Filename          string `toml:"file-name" json:"file-name"`                   // 日志文件名称
	FileMaxDays       int    `toml:"file-max-days" json:"file-max-days"`           // 日志最长保存天数
	FileMaxSize       int    `toml:"file-max-size" json:"file-max-size"`           // 单个文件最大长度
	CompressThreshold int    `toml:"compress-threshold" json:"compress-threshold"` // 启用sql文本压缩的阈值
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
	Enabled   bool   `toml:"enabled" json:"enabled"`     // 是否启用large sql跟踪
	Threshold int    `toml:"threshold" json:"threshold"` // 超出该长度的sql会记录到数据库某个位置
	SqlTypes  string `toml:"sql-types" json:"sql-types"` // 记录的sql类型
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

func initMCTechConfig() MCTech {
	return MCTech{
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
			SqlLog: SqlLog{
				Enabled:   false,
				MaxLength: 16 * 1024, // 默认最大记录16K
			},
			LargeSql: LargeSql{
				Enabled:   false,
				Threshold: 1 * 1024 * 1024,
				SqlTypes:  "delete,insert,update,select",
			},
		},
		SqlTrace: SqlTrace{
			Enabled:           false,
			FileMaxDays:       7,
			FileMaxSize:       300,
			CompressThreshold: 16 * 1024,
		},
	}
}
