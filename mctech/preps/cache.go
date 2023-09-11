package preps

import (
	"time"

	goCache "github.com/patrickmn/go-cache"
	"github.com/pingcap/tidb/util/intest"
)

// MCTechCache cache interface
type MCTechCache interface {
	Get(key string) (any, bool)
	Set(key string, value any)
}

type simpleMapCache struct {
	rawCache map[string]any
}

func (sc *simpleMapCache) Get(key string) (any, bool) {
	result, ok := sc.rawCache[key]
	return result, ok
}

func (sc *simpleMapCache) Set(key string, value any) {
	sc.rawCache[key] = value
}

type goCacheCache struct {
	rawCache *goCache.Cache
}

func (goc *goCacheCache) Get(key string) (any, bool) {
	result, ok := goc.rawCache.Get(key)
	return result, ok
}

func (goc *goCacheCache) Set(key string, value any) {
	goc.rawCache.Set(key, value, goCache.DefaultExpiration)
}

func (goc *goCacheCache) Init(expiration time.Duration, cleanup time.Duration) {
	goc.rawCache = goCache.New(expiration, cleanup)
}

// NewCache create Cache instance whitch implements MCTechCache interface.
func NewCache(expiration time.Duration, cleanup time.Duration) MCTechCache {
	if intest.InTest {
		return &simpleMapCache{rawCache: make(map[string]any)}
	}
	return &goCacheCache{rawCache: goCache.New(expiration, cleanup)}
}
