// add by zhangbing

package domain

import (
	"github.com/pingcap/errors"
	mctechworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// SetDenyDigestManagerForTest returns the deny digest manager on this domain
func (do *Domain) SetDenyDigestManagerForTest(mgr *mctechworker.DigestManager) {
	if !intest.InTest {
		err := errors.New("[EncodeForTest] not allow invoke")
		panic(err)
	}
	do.denyDigestManager.Store(&denyDigestManager{mgr})
}
