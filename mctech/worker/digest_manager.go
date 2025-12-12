package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"golang.org/x/exp/maps"
)

const (
	// MCTechDenyDigest is a table name
	MCTechDenyDigest = "mctech_deny_digest"
	// CreateMCTechDenyDigest is a table about deny sql digest
	CreateMCTechDenyDigest = `CREATE TABLE IF NOT EXISTS %n.%n (
		digest varchar(64) PRIMARY KEY,
		created_at datetime not null,
		expired_at datetime,
		last_request_time datetime NULL,
    query_sql longtext not null,
		remark text
	);`

	updateDigestRequestSQL = "UPDATE %n.%n SET last_request_time = %? WHERE digest = %?"
	selectDigestSQL        = "SELECT digest, expired_at from %n.%n where expired_at >= %?"
)

const digestManagerLoopTickerInterval = 10 * time.Second

// DenyDigestInfo 禁止执行的sql语句信息
type DenyDigestInfo struct {
	ExpiredAt       time.Time
	LastRequestTime *time.Time
}

type defaultDigestScheduler struct {
	ctx context.Context
	lck sync.RWMutex

	denyDigests map[string]*DenyDigestInfo
}

// Get get DenyDigestInfo
func (m *defaultDigestScheduler) Get(digest string) *DenyDigestInfo {
	failpoint.Inject("GetDenyDigestInfo", func(val failpoint.Value) {
		if val.(string) == digest {
			at := &DenyDigestInfo{ExpiredAt: time.Date(9999, 10, 1, 0, 0, 0, 0, time.Local)}
			failpoint.Return(at)
		}
	})
	m.lck.RLock()
	defer m.lck.RUnlock()

	return m.denyDigests[digest]
}

func (m *defaultDigestScheduler) SetAll(digests map[string]*DenyDigestInfo) {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.denyDigests = digests
}

func (m *defaultDigestScheduler) GetAll() map[string]*DenyDigestInfo {
	m.lck.RLock()
	defer m.lck.RUnlock()

	if m.denyDigests == nil {
		return nil
	}
	return maps.Clone(m.denyDigests)
}

func (m *defaultDigestScheduler) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	m.lck.RLock()
	defer m.lck.RUnlock()

	for digest, info := range m.denyDigests {
		if info.LastRequestTime == nil {
			continue
		}
		sql := updateDigestRequestSQL
		args := []any{
			mysql.SystemDB, MCTechDenyDigest,
			*info.LastRequestTime, digest,
		}
		if _, err := se.ExecuteInternal(ctx, sql, args...); err != nil {
			return fmt.Errorf("update digest request time error. execute sql: %s. %w", sql, err)
		}
		info.LastRequestTime = nil
	}
	return nil
}

func (m *defaultDigestScheduler) ReloadAll(se sqlexec.SQLExecutor) error {
	args := []any{
		mysql.SystemDB, MCTechDenyDigest,
		time.Now(),
	}
	rs, err := se.ExecuteInternal(m.ctx, selectDigestSQL, args...)
	if err != nil || rs == nil {
		return err
	}

	defer func() {
		terror.Log(rs.Close())
	}()

	rows, err := sqlexec.DrainRecordSet(m.ctx, rs, 8)
	if err != nil {
		return err
	}

	newMap := make(map[string]*DenyDigestInfo, len(rows))

	m.lck.Lock()
	defer m.lck.Unlock()

	for _, row := range rows {
		digest := row.GetString(0)
		at, err := row.GetTime(1).GoTime(time.Local)
		if err != nil {
			return err
		}

		var info *DenyDigestInfo
		if info = m.denyDigests[digest]; info == nil {
			info = &DenyDigestInfo{ExpiredAt: at}
		} else {
			info.ExpiredAt = at
		}
		newMap[digest] = info
	}
	m.denyDigests = newMap

	return nil
}

// DigestManager Digest Manager
type DigestManager struct {
	schedulerWrapper[string, DenyDigestInfo]
}

// Get method inplements Scheduler interface
func (m *DigestManager) Get(digest string) *DenyDigestInfo {
	return m.Unwrap().Get(digest)
}

// GetAll method inplements Scheduler interface
func (m *DigestManager) GetAll() map[string]*DenyDigestInfo {
	return m.Unwrap().GetAll()
}

// NewDigestManager creates a new digest manager
func NewDigestManager(sessPool sessionPool) *DigestManager {
	var scheduler schedulerWrapper[string, DenyDigestInfo]
	if config.GetMCTechConfig().SQLChecker.Enabled {
		ctx, cancel := context.WithCancel(context.Background())
		ctx = logutil.WithKeyValue(ctx, "deny-digest-worker", "deny-digest-manager")
		ctx = kv.WithInternalSourceType(ctx, "digestManager")
		scheduler = &defaultSchedulerWrapper[string, DenyDigestInfo]{
			ctx:                   ctx,
			cancel:                cancel,
			sessPool:              sessPool,
			scheduleTicker:        time.NewTicker(digestManagerLoopTickerInterval),
			updateHeartBeatTicker: time.NewTicker(digestManagerLoopTickerInterval),
			worker: &defaultDigestScheduler{
				ctx:         ctx,
				denyDigests: map[string]*DenyDigestInfo{},
			},
		}
	} else {
		scheduler = &nopSchedulerWrapper[string, DenyDigestInfo]{
			worker: &nonWorkerScheduler[string, DenyDigestInfo]{},
		}
	}
	return &DigestManager{scheduler}
}
