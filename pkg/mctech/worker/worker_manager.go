package worker

import (
	"context"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const sqlTimeout = 30 * time.Second

type workerStatus int

const (
	workerStatusCreated workerStatus = iota
	workerStatusRunning
	workerStatusStopping
	workerStatusStopped
)

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

type workerScheduler[T any] interface {
	GetAll() map[string]*T
	SetAll(all map[string]*T)
	ReloadAll(se sqlexec.SQLExecutor) error
	UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error
}

// Scheduler scheduler interface
type Scheduler interface {
	Start()
	Stop()
	WaitStopped(ctx context.Context, timeout time.Duration) error
}

type schedulerWrapper[T any] interface {
	Scheduler
	unwrap() workerScheduler[T]
}

type nopSchedulerWrapper[T any] struct {
}

func (m *nopSchedulerWrapper[T]) Start() {}

func (m *nopSchedulerWrapper[T]) Stop() {}

func (m *nopSchedulerWrapper[T]) WaitStopped(ctx context.Context, timeout time.Duration) error {
	return nil
}

func (m *nopSchedulerWrapper[T]) unwrap() workerScheduler[T] {
	return nil
}

type defaultSchedulerWrapper[T any] struct {
	sync.Mutex
	ctx    context.Context
	cancel func()
	err    error
	status workerStatus
	wg     util.WaitGroupWrapper

	sessPool              sessionPool
	scheduleTicker        *time.Ticker
	updateHeartBeatTicker *time.Ticker

	worker workerScheduler[T]
}

// Start start scheduler manager
func (m *defaultSchedulerWrapper[T]) Start() {
	m.Lock()
	defer m.Unlock()
	if m.status != workerStatusCreated {
		return
	}

	m.wg.Run(func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				logutil.BgLogger().Info("scheduler worker panic", zap.Any("recover", r), zap.Stack("stack"))
			}
			m.Lock()
			m.toStopped(err)
			m.Unlock()
		}()
		err = m.workerLoop()
	})
	m.status = workerStatusRunning
}

// Stop stop scheduler
func (m *defaultSchedulerWrapper[T]) Stop() {
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
func (m *defaultSchedulerWrapper[T]) Status() workerStatus {
	m.Lock()
	defer m.Unlock()
	return m.status
}

// WaitStopped get wait stop
func (m *defaultSchedulerWrapper[T]) WaitStopped(ctx context.Context, timeout time.Duration) error {
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

func (m *defaultSchedulerWrapper[T]) unwrap() workerScheduler[T] {
	return m.worker
}

func (m *defaultSchedulerWrapper[T]) toStopped(err error) {
	m.status = workerStatusStopped
	m.err = err

	m.scheduleTicker.Stop()
	m.updateHeartBeatTicker.Stop()
}

func (m *defaultSchedulerWrapper[T]) workerLoop() (err error) {
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
	if err = m.worker.ReloadAll(se); err != nil {
		logutil.Logger(m.ctx).Warn("fail to reload data", zap.Error(err))
	}

	for {
		select {
		// misc
		case <-m.ctx.Done():
			return nil
		case <-m.updateHeartBeatTicker.C:
			updateHeartBeatCtx, cancel := context.WithTimeout(m.ctx, sqlTimeout)
			if err = m.worker.UpdateHeartBeat(updateHeartBeatCtx, se); err != nil {
				logutil.Logger(m.ctx).Warn("fail to update job heart beat", zap.Error(err))
			}
			cancel()
		case <-m.scheduleTicker.C:
			if err = m.worker.ReloadAll(se); err != nil {
				logutil.Logger(m.ctx).Warn("fail to reload data", zap.Error(err))
			}
		}
	}
}

var (
	_ schedulerWrapper[any] = &nopSchedulerWrapper[any]{}
	_ schedulerWrapper[any] = &defaultSchedulerWrapper[any]{}
)
