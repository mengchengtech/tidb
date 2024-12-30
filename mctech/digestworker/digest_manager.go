package digestworker

import (
	"context"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const updateDigestRequestTemplate = "UPDATE mysql.mctech_deny_digest SET last_request_time = %? WHERE digest = %?"
const selectDigestTemplate = "SELECT digest, expired_at from mysql.mctech_deny_digest where expired_at >= %?"

const digestManagerLoopTickerInterval = 10 * time.Second
const digestSQLTimeout = 30 * time.Second

type workerStatus int

const (
	workerStatusCreated workerStatus = iota
	workerStatusRunning
	workerStatusStopping
	workerStatusStopped
)

type denyDigestInfo struct {
	expiredAt       time.Time
	lastRequestTime *time.Time
}

func (d *denyDigestInfo) ExpiredAt() time.Time {
	return d.expiredAt
}

func (d *denyDigestInfo) LastRequestTime() *time.Time {
	return d.lastRequestTime
}

func (d *denyDigestInfo) SetLastRequestTime(t time.Time) {
	d.lastRequestTime = &t
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// DigestManager Digest Manager
type DigestManager struct {
	sync.Mutex
	ctx    context.Context
	cancel func()
	err    error
	status workerStatus
	wg     util.WaitGroupWrapper

	sessPool              sessionPool
	denyDigests           map[string]*denyDigestInfo
	scheduleTicker        *time.Ticker
	updateHeartBeatTicker *time.Ticker
}

// Start start digest manager
func (m *DigestManager) Start() {
	m.Lock()
	defer m.Unlock()
	if m.status != workerStatusCreated {
		return
	}

	m.wg.Run(func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Info("digest worker panic", zap.Any("recover", r), zap.Stack("stack"))
			}
			m.Lock()
			m.toStopped(err)
			m.Unlock()
		}()
		err = m.digestLoop()
	})
	m.status = workerStatusRunning
}

// Stop stop digest manager
func (m *DigestManager) Stop() {
	m.Lock()
	defer m.Unlock()
	switch m.status {
	case workerStatusCreated:
		m.cancel()
		m.toStopped(nil)
	case workerStatusRunning:
		m.cancel()
		m.status = workerStatusStopping
	}
}

// Status get status
func (m *DigestManager) Status() workerStatus {
	m.Lock()
	defer m.Unlock()
	return m.status
}

// WaitStopped get wait stop
func (m *DigestManager) WaitStopped(ctx context.Context, timeout time.Duration) error {
	if m.Status() == workerStatusStopped {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	go func() {
		m.wg.Wait()
		cancel()
	}()

	<-ctx.Done()
	if m.Status() != workerStatusStopped {
		return ctx.Err()
	}
	return nil
}

// Get get denyDigestInfo
func (m *DigestManager) Get(digest string) *denyDigestInfo {
	failpoint.Inject("GetDenyDigestInfo", func(val failpoint.Value) {
		if val.(string) == digest {
			at := &denyDigestInfo{expiredAt: time.Date(9999, 10, 1, 0, 0, 0, 0, time.Local)}
			failpoint.Return(at)
		}
	})
	return m.denyDigests[digest]
}

func (m *DigestManager) toStopped(err error) {
	m.status = workerStatusStopped
	m.err = err

	m.scheduleTicker.Stop()
	m.updateHeartBeatTicker.Stop()
}

func (m *DigestManager) digestLoop() (err error) {
	var resource pools.Resource
	if resource, err = m.sessPool.Get(); err != nil {
		return err
	}

	var se sqlexec.SQLExecutor
	se, ok := resource.(sqlexec.SQLExecutor)
	if !ok {
		m.sessPool.Put(resource)
		return errors.Errorf("%T cannot be casted to sqlexec.SQLExecutor", resource)
	}

	// 启动时先执行一次
	if err = m.reloadDenyDigests(se); err != nil {
		logutil.Logger(m.ctx).Warn("fail to reload deny digests", zap.Error(err))
	}

	for {
		select {
		// misc
		case <-m.ctx.Done():
			return nil
		case <-m.updateHeartBeatTicker.C:
			updateHeartBeatCtx, cancel := context.WithTimeout(m.ctx, digestSQLTimeout)
			if err = m.updateHeartBeat(updateHeartBeatCtx, se); err != nil {
				logutil.Logger(m.ctx).Warn("fail to update job heart beat", zap.Error(err))
			}
			cancel()
		case <-m.scheduleTicker.C:
			if err = m.reloadDenyDigests(se); err != nil {
				logutil.Logger(m.ctx).Warn("fail to reload deny digests", zap.Error(err))
			}
		}
	}
}

func (m *DigestManager) updateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	ctx = kv.WithInternalSourceType(ctx, "digestManager")
	for digest, info := range m.denyDigests {
		if info.lastRequestTime == nil {
			continue
		}
		sql := updateDigestRequestTemplate
		args := []any{*info.lastRequestTime, digest}
		if _, err := se.ExecuteInternal(ctx, sql, args...); err != nil {
			return errors.Wrapf(err, "execute sql: %s", sql)
		}
		info.lastRequestTime = nil
	}
	return nil
}

func (m *DigestManager) reloadDenyDigests(se sqlexec.SQLExecutor) error {
	ctx := kv.WithInternalSourceType(context.Background(), "digestManager")
	rs, err := se.ExecuteInternal(ctx, selectDigestTemplate, time.Now())
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

	newMap := make(map[string]*denyDigestInfo, len(rows))
	for _, row := range rows {
		digest := row.GetString(0)
		at, err := row.GetTime(1).GoTime(time.Local)

		var info *denyDigestInfo
		if info = m.denyDigests[digest]; info == nil {
			if err != nil {
				return err
			}
			info = &denyDigestInfo{expiredAt: at}
		} else {
			info.expiredAt = at
		}
		newMap[digest] = info
	}
	m.denyDigests = newMap

	return nil
}

// NewDigestManager creates a new digest manager
func NewDigestManager(sessPool sessionPool) *DigestManager {
	manager := &DigestManager{
		sessPool:              sessPool,
		denyDigests:           make(map[string]*denyDigestInfo, 0),
		scheduleTicker:        time.NewTicker(digestManagerLoopTickerInterval),
		updateHeartBeatTicker: time.NewTicker(digestManagerLoopTickerInterval),
	}

	ctx, cancel := context.WithCancel(context.Background())
	manager.ctx = logutil.WithKeyValue(ctx, "deny-digest-worker", "deny-digest-manager")
	manager.cancel = cancel

	return manager
}
