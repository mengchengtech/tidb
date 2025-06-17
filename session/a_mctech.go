// add by zhangbing

package session

import (
	"errors"
	"fmt"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/intest"
)

// CheckSQLDigest check sql digest is deny
func CheckSQLDigest(sctx sessionctx.Context, digest string) error {
	if sctx.GetSessionVars().InRestrictedSQL {
		return nil
	}

	dom := domain.GetDomain(sctx)
	var (
		mgr domain.DenyDigestManager
		ok  bool
	)
	if mgr, ok = dom.DenyDigestManager(); !ok {
		if !intest.InTest {
			return errors.New("Domain.denyDigestManager is nil")
		}
		return nil
	}

	var info domain.DenyDigestInfo
	if info, ok = mgr.Get(digest); !ok {
		return nil
	}

	now := time.Now()
	info.SetLastRequestTime(now)
	if deny := now.Before(info.ExpiredAt()); deny {
		return fmt.Errorf("current sql is rejected and resumed at '%s' . digest: %s", info.ExpiredAt().Format("2006-01-02 15:04:05.0000"), digest)
	}
	return nil
}
