package worker

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// DenyDigests returns the deny digests
func (m *DigestManager) GetAll() map[string]*DenyDigestInfo {
	return m.unwrap().(*defaultDigestScheduler).GetAll()
}

// DenyDigests returns the deny digests
func (m *DigestManager) SetAll(denyDigests map[string]*DenyDigestInfo) {
	m.unwrap().(*defaultDigestScheduler).SetAll(denyDigests)
}

// RescheduleJobs is an exported version of rescheduleJobs for test
func (m *DigestManager) ReloadAll(se sqlexec.SQLExecutor) error {
	return m.unwrap().(*defaultDigestScheduler).ReloadAll(se)
}

// UpdateHeartBeat is an exported version of updateHeartBeat for test
func (m *DigestManager) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	return m.unwrap().(*defaultDigestScheduler).UpdateHeartBeat(ctx, se)
}

// DenyDigests returns the deny digests
func (m *CrossDBManager) GetAll() map[string]*ServiceCrossDBInfo {
	return m.unwrap().(*defaultCrossDBScheduler).GetAll()
}

// DenyDigests returns the deny digests
func (m *CrossDBManager) SetAll(cross map[string]*ServiceCrossDBInfo) {
	m.unwrap().(*defaultCrossDBScheduler).SetAll(cross)
}

// RescheduleJobs is an exported version of rescheduleJobs for test
func (m *CrossDBManager) ReloadAll(se sqlexec.SQLExecutor) error {
	return m.unwrap().(*defaultCrossDBScheduler).ReloadAll(se)
}

// UpdateHeartBeat is an exported version of updateHeartBeat for test
func (m *CrossDBManager) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	return m.unwrap().(*defaultCrossDBScheduler).UpdateHeartBeat(ctx, se)
}
