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
	SequenceMock          bool
	SequenceDebug         bool
	SequenceMaxFetchCount int64
	SequenceBackend       int64
	SequenceAPIPrefix     string

	// encryption
	EncryptionMock      bool
	EncryptionAccessID  string
	EncryptionAPIPrefix string

	TenantEnabled bool

	DbCheckerEnabled          bool
	DbCheckerAPIPrefix        string
	DbCheckerMutexAcrossDbs   []string
	DbCheckerExcludeAcrossDbs []string
	DbCheckerAcrossDbGroups   []string
}

var mctechOpts *Option

// GetOption get mctech option
func GetOption() *Option {
	// 只能懒加载，需要在启动时先加载 config模块
	once := &sync.Once{}
	once.Do(func() {
		if mctechOpts == nil {
			mctechOpts = initMCTechOption()
		}
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
		DbCheckerEnabled:          opts.DbChecker.Enabled,
		DbCheckerAPIPrefix:        formatURL(opts.DbChecker.APIPrefix),
		DbCheckerMutexAcrossDbs:   opts.DbChecker.MutexAcrossDbs,
		DbCheckerExcludeAcrossDbs: opts.DbChecker.ExcludeAcrossDbs,
		DbCheckerAcrossDbGroups:   opts.DbChecker.AcrossDbGroups,
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
