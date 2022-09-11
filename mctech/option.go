package mctech

import (
	"net/url"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"go.uber.org/zap"
)

// Option mctech option
type Option struct {
	// sequence是否使用mock模式，不执行rpc调用，从本地返回固定的值
	SequenceMock bool
	// 是否开启sequence取值过程的调试模式，输出更多的日志
	SequenceDebug bool
	// 每次rpc调用获取sequence的最大个数
	SequenceMaxFetchCount int64
	// 后台并行获取sequence的最大并发数
	SequenceBackend int64
	// sequence服务的调用地址前缀
	SequenceAPIPrefix string

	// encryption
	EncryptionMock     bool
	EncryptionAccessID string
	// encryption服务的调用地址前缀
	EncryptionAPIPrefix string

	// 是否启用租户隔离
	TenantEnabled bool
	// 禁用Prepare/Execute语句
	ForbiddenPrepare bool

	// 是否开启同一sql语句中引用的数据库共存约束检查
	DbCheckerEnabled bool
	//
	DbCheckerMutexAcrossDbs []string
	// 被排除在约束检查外的数据库名称
	DbCheckerExcludeAcrossDbs []string
	DbCheckerAcrossDbGroups   []string
	// 获取global_dw_*的当前索引的服务地址前缀
	DbCheckerAPIPrefix string

	// 是否开启 create table自动插入特定的version列的特性
	DDLVersionColumnEnabled bool
	// version列的名称
	DDLVersionColumnName string
	// 插入version的表需要满足的条件
	DDLVersionFilters []string

	// 是否支持dbPrefix hint
	MsicDbPrefixEnabled bool
}

var mctechOpts *Option

// GetOption get mctech option
func GetOption() *Option {
	if mctechOpts != nil {
		return mctechOpts
	}
	// 只能懒加载，需要在启动时先加载 config模块
	once := &sync.Once{}
	once.Do(func() {
		mctechOpts = initMCTechOption()
	})
	return mctechOpts
}

// SetOptionForTest for unit test
func SetOptionForTest(option *Option) {
	mctechOpts = option
}

func initMCTechOption() *Option {
	opts := config.GetGlobalConfig().MCTech
	option := &Option{
		SequenceMock:          opts.Sequence.Mock,
		SequenceDebug:         opts.Sequence.Debug,
		SequenceMaxFetchCount: opts.Sequence.MaxFetchCount,
		SequenceBackend:       opts.Sequence.Backend,
		SequenceAPIPrefix:     formatURL(opts.Sequence.APIPrefix),

		EncryptionMock:      opts.Encryption.Mock,
		EncryptionAccessID:  opts.Encryption.AccessID,
		EncryptionAPIPrefix: formatURL(opts.Encryption.APIPrefix),

		TenantEnabled:             opts.Tenant.Enabled,
		ForbiddenPrepare:          opts.Tenant.ForbiddenPrepare,
		DbCheckerEnabled:          opts.DbChecker.Enabled,
		DbCheckerAPIPrefix:        formatURL(opts.DbChecker.APIPrefix),
		DbCheckerMutexAcrossDbs:   opts.DbChecker.MutexAcrossDbs,
		DbCheckerExcludeAcrossDbs: opts.DbChecker.ExcludeAcrossDbs,
		DbCheckerAcrossDbGroups:   opts.DbChecker.AcrossDbGroups,

		DDLVersionColumnEnabled: opts.DDL.Version.Enabled,
		DDLVersionColumnName:    opts.DDL.Version.Name,
		DDLVersionFilters:       opts.DDL.Version.DbMatches,
	}

	return option
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
