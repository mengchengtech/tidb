package udf

import (
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"go.uber.org/zap"
)

func getBackendCount() int64 {
	backend := config.GetGlobalConfig().MCTech.Sequence.Backend
	log.Info("Get backend sequence goroutines.",
		zap.Int64("backend", backend))
	return backend
}

func getSequenceServiceUrlPrefix() string {
	// 先从配置中获取
	apiPrefix := config.GetGlobalConfig().MCTech.Sequence.ApiPrefix
	if apiPrefix == "" {
		apiPrefix = "http://node-infra-sequence-service.mc/"
	}

	apiPrefix = formatUrl(apiPrefix)
	log.Info("Get sequence service api prefix.",
		zap.String("apiPrefix", apiPrefix))
	return apiPrefix
}

func getAesAccessId() string {
	// 先从配置中获取
	accessId := config.GetGlobalConfig().MCTech.Encryption.AccessId
	if accessId == "" {
		accessId = "oJEKJh1wvqncJYASxp1Iiw"
	}
	return accessId
}

func getEncryptionServiceUrlPrefix() string {
	// 先从配置中获取
	apiPrefix := config.GetGlobalConfig().MCTech.Encryption.ApiPrefix
	if apiPrefix == "" {
		apiPrefix = "http://node-infra-encryption-service.mc/"
	}
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
