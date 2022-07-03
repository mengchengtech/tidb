package mctech

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"go.uber.org/zap"
)

type MCTechOption struct {
	Sequence_Mock          bool
	Sequence_Debug         bool
	Sequence_MaxFetchCount int64
	Sequence_Backend       int64
	Sequence_ApiPrefix     string

	// encryption
	Encryption_Mock      bool
	Encryption_AccessId  string
	Encryption_ApiPrefix string

	DbChecker_ApiPrefix        string
	DbChecker_MutexAcrossDbs   []string
	DbChecker_ExcludeAcrossDbs []string
	DbChecker_AcrossDbGroups   []string
}

var mctechOpts *MCTechOption

func GetOption() *MCTechOption {
	// 只能懒加载，需要在启动时先加载 config模块
	once := &sync.Once{}
	once.Do(func() {
		if mctechOpts == nil {
			mctechOpts = initMCTechOption()
		}
	})
	return mctechOpts
}

func initMCTechOption() *MCTechOption {
	opts := config.GetGlobalConfig().MCTech
	option := &MCTechOption{
		Sequence_Mock:          opts.Sequence.Mock,
		Sequence_Debug:         opts.Sequence.Debug,
		Sequence_MaxFetchCount: opts.Sequence.MaxFetchCount,
		Sequence_Backend:       opts.Sequence.Backend,
		Sequence_ApiPrefix:     formatUrl(opts.Sequence.ApiPrefix),

		Encryption_Mock:      opts.Encryption.Mock,
		Encryption_AccessId:  opts.Encryption.AccessId,
		Encryption_ApiPrefix: formatUrl(opts.Encryption.ApiPrefix),

		DbChecker_ApiPrefix:        formatUrl(opts.DbChecker.ApiPrefix),
		DbChecker_MutexAcrossDbs:   opts.DbChecker.MutexAcrossDbs,
		DbChecker_ExcludeAcrossDbs: opts.DbChecker.ExcludeAcrossDbs,
		DbChecker_AcrossDbGroups:   opts.DbChecker.AcrossDbGroups,
	}

	content, err := json.Marshal(option)
	if err != nil {
		panic(err)
	}

	log.Info(fmt.Sprintf("mctech options: %s", content))
	return option
}

func formatUrl(str string) string {
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
