// add by zhangbing

package domain

import (
	"context"
	"time"

	mcworker "github.com/pingcap/tidb/mctech/worker"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// DenyDigestManager DenyDigestManager interface
type DenyDigestManager interface {
	// Get 是否拒绝执行 digest 对应的sql
	Get(digest string) *mcworker.DenyDigestInfo
}

type denyDigestManager struct {
	mgr *mcworker.DigestManager
}

func (m *denyDigestManager) Get(digest string) *mcworker.DenyDigestInfo {
	return m.mgr.Get(digest)
}

// CrossDBManager CrossDBManager interface
type CrossDBManager interface {
	Exclude(dbNames []string) []string
	Get(pattern mcworker.SQLInvokerPattern) *mcworker.CrossDBInfo
	GetAll() map[string]*mcworker.CrossDBInfo
	GetLoadedResults() []*mcworker.LoadedRuleResult
}

type crossDBManager struct {
	mgr *mcworker.CrossDBManager
}

func (m *crossDBManager) Exclude(dbNames []string) []string {
	return m.mgr.Exclude(dbNames)
}

func (m *crossDBManager) Get(pattern mcworker.SQLInvokerPattern) *mcworker.CrossDBInfo {
	return m.mgr.Get(pattern)
}

func (m *crossDBManager) GetAll() map[string]*mcworker.CrossDBInfo {
	return m.mgr.GetAll()
}

func (m *crossDBManager) GetLoadedResults() []*mcworker.LoadedRuleResult {
	return m.mgr.GetLoadedResults()
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

// StartCrossDBManager creates and starts the cross db manager
func (do *Domain) StartCrossDBManager() {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("crossDBManager exited.")
		}()

		mgr := mcworker.NewCrossDBManager(do.sysSessionPool)
		do.crossDBManager.Store(&crossDBManager{mgr: mgr})
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
			logutil.BgLogger().Warn("fail to wait until the cross db manager stop", zap.Error(err))
		}
	}, "crossDBManager")
}

// CrossDBManager returns the cross db manager on this domain
func (do *Domain) CrossDBManager() (CrossDBManager, bool) {
	mgr := do.crossDBManager.Load()
	return mgr, mgr != nil
}
