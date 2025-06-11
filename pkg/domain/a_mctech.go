// add by zhangbing

package domain

import (
	"context"
	"time"

	mcworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DenyDigestInfo DenyDigestInfo interface
type DenyDigestInfo interface {
	// 被拒绝执行的sql配置的digest失效时间
	ExpiredAt() time.Time
	// 设置最后一次请求时间
	SetLastRequestTime(time.Time)
}

// DenyDigestManager DenyDigestManager interface
type DenyDigestManager interface {
	// Get 是否拒绝执行 digest 对应的sql
	Get(digest string) (DenyDigestInfo, bool)
}

type denyDigestManager struct {
	mgr *mcworker.DigestManager
}

func (m *denyDigestManager) Get(digest string) (DenyDigestInfo, bool) {
	info := m.mgr.Get(digest)
	return info, info != nil
}

// StartDenyDigestManager creates and starts the deny digest manager
func (do *Domain) StartDenyDigestManager() {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("denyDigestManager exited.")
		}()

		mgr := mcworker.NewDigestManager(do.sysSessionPool)
		do.denyDigestManager.Store(&denyDigestManager{mgr: mgr})
		mgr.Start()

		<-do.exit

		mgr.Stop()
		err := mgr.WaitStopped(context.Background(), func() time.Duration {
			if intest.InTest {
				return 10 * time.Second
			}
			return 30 * time.Second
		}())
		if err != nil {
			logutil.BgLogger().Warn("fail to wait until the deny digest manager stop", zap.Error(err))
		}
	}, "denyDigestManager")
}

// DenyDigestManager returns the deny digest manager on this domain
func (do *Domain) DenyDigestManager() (DenyDigestManager, bool) {
	mgr := do.denyDigestManager.Load()
	return mgr, mgr != nil
}
