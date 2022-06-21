package udf

import (
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"go.uber.org/zap"
)

type mctechOption struct {
}

func (o *mctechOption) SequenceMock() bool {
	mock := config.GetGlobalConfig().MCTech.Sequence.Mock
	log.Info("mock sequence service ?", zap.Bool("mock", mock))
	return mock
}

func (o *mctechOption) SequenceDebug() bool {
	debug := config.GetGlobalConfig().MCTech.Sequence.Debug
	log.Info("debug sequence service ?", zap.Bool("debug", debug))
	return debug
}

func (o *mctechOption) MaxFetchCount() int64 {
	maxFetchCount := config.GetGlobalConfig().MCTech.Sequence.MaxFetchCount
	return maxFetchCount
}

func (o *mctechOption) BackendCount() int64 {
	backend := config.GetGlobalConfig().MCTech.Sequence.Backend
	log.Info("Get backend sequence goroutines.",
		zap.Int64("backend", backend))
	return backend
}

func (o *mctechOption) SequenceServiceUrlPrefix() string {
	// 先从配置中获取
	apiPrefix := config.GetGlobalConfig().MCTech.Sequence.ApiPrefix
	apiPrefix = formatUrl(apiPrefix)
	log.Info("Get sequence service api prefix.",
		zap.String("apiPrefix", apiPrefix))
	return apiPrefix
}

func (o *mctechOption) EncryptionMock() bool {
	mock := config.GetGlobalConfig().MCTech.Encryption.Mock
	log.Info("mock sequence service ?", zap.Bool("mock", mock))
	return mock
}

func (o *mctechOption) EncryptionAesAccessId() string {
	// 先从配置中获取
	accessId := config.GetGlobalConfig().MCTech.Encryption.AccessId
	return accessId
}

func (o *mctechOption) EncryptionServiceUrlPrefix() string {
	// 先从配置中获取
	apiPrefix := config.GetGlobalConfig().MCTech.Encryption.ApiPrefix
	apiPrefix = formatUrl(apiPrefix)
	log.Info("Get crypto service api prefix.",
		zap.String("apiPrefix", apiPrefix))
	return apiPrefix
}

func formatUrl(str string) string {
	u, err := url.Parse(str)
	if err != nil {
		log.Error("apiPrefix format error.",
			zap.String("apiPrefix", str),
			zap.Error(err))
		panic(err)
	}

	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	apiPrefix := u.String()
	return apiPrefix
}

var option = new(mctechOption)
