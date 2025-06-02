// add by zhangbing

package domain

import (
	"context"
	"time"

	mctechworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DenyDigestManager DenyDigestManager interface
type DenyDigestManager interface {
	// Get 是否拒绝执行 digest 对应的sql
	Get(digest string) (*mctechworker.DenyDigestInfo, bool)
}

type denyDigestManager struct {
	mgr *mctechworker.DigestManager
}

func (m *denyDigestManager) Get(digest string) (*mctechworker.DenyDigestInfo, bool) {
	info := m.mgr.Get(digest)
	return info, info != nil
}

// ServiceCrossDBManager ServiceCrossDBManager interface
type ServiceCrossDBManager interface {
	Get(service string) (*mctechworker.ServiceCrossDBInfo, bool)
}

type serviceCrossDBManager struct {
	mgr *mctechworker.CrossDBManager
}

func (m *serviceCrossDBManager) Get(service string) (*mctechworker.ServiceCrossDBInfo, bool) {
	info := m.mgr.Get(service)
	return info, info != nil
}

// StartDenyDigestManager creates and starts the deny digest manager
func (do *Domain) StartDenyDigestManager() {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("denyDigestManager exited.")
		}()

		mgr := mctechworker.NewDigestManager(do.sysSessionPool)
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

// StartServiceCrossDBManager creates and starts the service cross db manager
func (do *Domain) StartServiceCrossDBManager() {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("serviceCrossDBManager exited.")
		}()

		mgr := mctechworker.NewCrossDBManager(do.sysSessionPool)
		do.serviceCrossDBManager.Store(&serviceCrossDBManager{mgr: mgr})
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
			logutil.BgLogger().Warn("fail to wait until the service cross db manager stop", zap.Error(err))
		}
	}, "serviceCrossDBManager")
}

// ServiceCrossDBManager returns the service cross db manager on this domain
func (do *Domain) ServiceCrossDBManager() (ServiceCrossDBManager, bool) {
	mgr := do.serviceCrossDBManager.Load()
	return mgr, mgr != nil
}
