package digestworker

import (
	"context"

	"github.com/pingcap/tidb/util/sqlexec"
)

// DenyDigestInfo exports the denyDigestInfo for test
type DenyDigestInfo = denyDigestInfo

// DenyDigests returns the deny digests
func (m *DigestManager) DenyDigests() map[string]*DenyDigestInfo {
	return m.denyDigests
}

// DenyDigests returns the deny digests
func (m *DigestManager) SetDenyDigests(denyDigests map[string]*DenyDigestInfo) {
	m.denyDigests = denyDigests
}

// RescheduleJobs is an exported version of rescheduleJobs for test
func (m *DigestManager) ReloadDenyDigests(se sqlexec.SQLExecutor) {
	m.reloadDenyDigests(se)
}

// UpdateHeartBeat is an exported version of updateHeartBeat for test
func (m *DigestManager) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	return m.updateHeartBeat(ctx, se)
}
