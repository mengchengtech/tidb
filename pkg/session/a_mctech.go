// add by zhangbing

package session

import (
	"errors"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var (
	// createMCTechDenyDigest is a table about deny sql digest
	createMCTechDenyDigest = `CREATE TABLE IF NOT EXISTS mysql.mctech_deny_digest (
		digest varchar(64) PRIMARY KEY,
		created_at datetime not null,
		expired_at datetime,
		last_request_time datetime NULL,
    query_sql longtext not null,
		remark text
	);`
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
