// add by zhangbing

package domain

import (
	"github.com/pingcap/errors"
	mcworker "github.com/pingcap/tidb/mctech/worker"
	"github.com/pingcap/tidb/util/intest"
)

// SetDenyDigestManagerForTest returns the deny digest manager on this domain
func (do *Domain) SetDenyDigestManagerForTest(mgr *mcworker.DigestManager) {
	if !intest.InTest {
		err := errors.New("[EncodeForTest] not allow invoke")
		panic(err)
	}
	do.denyDigestManager.Store(&denyDigestManager{mgr})
}

// SetServiceCrossDBManagerForTest returns the deny digest manager on this domain
func (do *Domain) SetServiceCrossDBManagerForTest(mgr *mcworker.CrossDBManager) {
	if !intest.InTest {
		err := errors.New("[EncodeForTest] not allow invoke")
		panic(err)
	}
	do.crossDBManager.Store(&crossDBManager{mgr})
}
