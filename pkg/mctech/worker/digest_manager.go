package worker

import (
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pkg/errors"
)

const (
	// CreateMCTechDenyDigest is a table about deny sql digest
	CreateMCTechDenyDigest = `CREATE TABLE IF NOT EXISTS mysql.mctech_deny_digest (
		digest varchar(64) PRIMARY KEY,
		created_at datetime not null,
		expired_at datetime,
		last_request_time datetime NULL,
    query_sql longtext not null,
		remark text
	);`

	updateDigestRequestTemplate = "UPDATE mysql.mctech_deny_digest SET last_request_time = %? WHERE digest = %?"
	selectDigestTemplate        = "SELECT digest, expired_at from mysql.mctech_deny_digest where expired_at >= %?"
)
const digestManagerLoopTickerInterval = 10 * time.Second

// DenyDigestInfo 禁止执行的sql语句信息
type DenyDigestInfo struct {
	ExpiredAt       time.Time
	LastRequestTime *time.Time
}

type defaultDigestScheduler struct {
	ctx         context.Context
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
	return m.denyDigests[digest]
}

func (m *defaultDigestScheduler) SetAll(digests map[string]*DenyDigestInfo) {
	m.denyDigests = digests
}

func (m *defaultDigestScheduler) GetAll() map[string]*DenyDigestInfo {
	return m.denyDigests
}

func (m *defaultDigestScheduler) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	for digest, info := range m.denyDigests {
		if info.LastRequestTime == nil {
			continue
		}
		sql := updateDigestRequestTemplate
		args := []any{*info.LastRequestTime, digest}
		if _, err := se.ExecuteInternal(ctx, sql, args...); err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}
		info.LastRequestTime = nil
	}
	return nil
}

func (m *defaultDigestScheduler) ReloadAll(se sqlexec.SQLExecutor) error {
	rs, err := se.ExecuteInternal(m.ctx, selectDigestTemplate, time.Now())
	if err != nil {
		return err
	}

	if rs == nil {
		return nil
	}

	defer func() {
		terror.Log(rs.Close())
	}()

	rows, err := sqlexec.DrainRecordSet(m.ctx, rs, 8)
	if err != nil {
		return err
	}

	newMap := make(map[string]*DenyDigestInfo, len(rows))
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
	schedulerWrapper[DenyDigestInfo]
}

// Get method inplements Scheduler interface
func (m *DigestManager) Get(digest string) *DenyDigestInfo {
	s := m.unwrap()
	if s != nil {
		return s.(*defaultDigestScheduler).Get(digest)
	}
	return nil
}

// NewDigestManager creates a new digest manager
func NewDigestManager(sessPool sessionPool) *DigestManager {
	var scheduler schedulerWrapper[DenyDigestInfo]
	if config.GetMCTechConfig().SQLChecker.Enabled {
		ctx, cancel := context.WithCancel(context.Background())
		ctx = logutil.WithKeyValue(ctx, "deny-digest-worker", "deny-digest-manager")
		ctx = kv.WithInternalSourceType(ctx, "digestManager")
		scheduler = &defaultSchedulerWrapper[DenyDigestInfo]{
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
		scheduler = &nopSchedulerWrapper[DenyDigestInfo]{}
	}
	return &DigestManager{scheduler}
}
