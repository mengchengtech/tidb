package worker

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

type CrossDBData = crossDBData

// DenyDigests returns the deny digests
func (m *DigestManager) GetAll() map[string]*DenyDigestInfo {
	return m.Unwrap().GetAll()
}

// DenyDigests returns the deny digests
func (m *DigestManager) SetAll(denyDigests map[string]*DenyDigestInfo) {
	m.Unwrap().SetAll(denyDigests)
}

// RescheduleJobs is an exported version of rescheduleJobs for test
func (m *DigestManager) ReloadAll(se sqlexec.SQLExecutor) error {
	return m.Unwrap().ReloadAll(se)
}

// UpdateHeartBeat is an exported version of updateHeartBeat for test
func (m *DigestManager) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	return m.Unwrap().UpdateHeartBeat(ctx, se)
}

// DenyDigests returns the deny digests
func (m *CrossDBManager) GetAll() map[string]*CrossDBInfo {
	return m.Unwrap().GetAll()
}

// DenyDigests returns the deny digests
func (m *CrossDBManager) SetAll(cross map[string]*CrossDBInfo) {
	m.Unwrap().SetAll(cross)
}

// RescheduleJobs is an exported version of rescheduleJobs for test
func (m *CrossDBManager) ReloadAll(se sqlexec.SQLExecutor) error {
	return m.Unwrap().ReloadAll(se)
}

// UpdateHeartBeat is an exported version of updateHeartBeat for test
func (m *CrossDBManager) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	return m.Unwrap().UpdateHeartBeat(ctx, se)
}
