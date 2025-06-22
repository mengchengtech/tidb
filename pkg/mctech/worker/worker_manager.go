package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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

type workerScheduler[TKey, TValue any] interface {
	Get(key TKey) *TValue
	GetAll() map[string]*TValue
	ReloadAll(se sqlexec.SQLExecutor) error
	UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error
}

// Scheduler scheduler interface
type Scheduler interface {
	Start()
	Stop()
	WaitStopped(ctx context.Context, timeout time.Duration) error
}

type schedulerWrapper[TKey, TValue any] interface {
	Scheduler
	Unwrap() workerScheduler[TKey, TValue]
}

type nopSchedulerWrapper[TKey, TValue any] struct {
	worker workerScheduler[TKey, TValue]
}

func (m *nopSchedulerWrapper[TKey, TValue]) Start() {}

func (m *nopSchedulerWrapper[TKey, TValue]) Stop() {}

func (m *nopSchedulerWrapper[TKey, TValue]) WaitStopped(ctx context.Context, timeout time.Duration) error {
	return nil
}

func (m *nopSchedulerWrapper[TKey, TValue]) Unwrap() workerScheduler[TKey, TValue] {
	return m.worker
}

type nonWorkerScheduler[TKey, TValue any] struct {
}

func (s *nonWorkerScheduler[TKey, TValue]) Get(key string) *TValue {
	return nil
}

func (s *nonWorkerScheduler[TKey, TValue]) GetAll() map[string]*TValue {
	return nil
}

func (s *nonWorkerScheduler[TKey, TValue]) SetAll(all map[string]*TValue) {
}

func (s *nonWorkerScheduler[TKey, TValue]) ReloadAll(se sqlexec.SQLExecutor) error {
	return nil
}

func (s *nonWorkerScheduler[TKey, TValue]) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	return nil
}

type defaultSchedulerWrapper[TKey, TValue any] struct {
	sync.Mutex
	ctx    context.Context
	cancel func()
	err    error
	status workerStatus
	wg     util.WaitGroupWrapper

	sessPool              sessionPool
	scheduleTicker        *time.Ticker
	updateHeartBeatTicker *time.Ticker

	worker workerScheduler[TKey, TValue]
}

// Start start scheduler manager
func (m *defaultSchedulerWrapper[TKey, TValue]) Start() {
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
			defer m.Unlock()
			m.toStopped(err)
		}()
		err = m.workerLoop()
	})
	m.status = workerStatusRunning

	// 启动服务时强制选执行一次
	var (
		se  sqlexec.SQLExecutor
		err error
	)
	if se, err = m.getSQLExecutor(); err == nil {
		err = m.worker.ReloadAll(se)
	}

	if err != nil {
		logutil.BgLogger().Info("scheduler first load error", zap.Error(err), zap.Stack("stack"))
	}
}

// Stop stop scheduler
func (m *defaultSchedulerWrapper[TKey, TValue]) Stop() {
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
func (m *defaultSchedulerWrapper[TKey, TValue]) Status() workerStatus {
	m.Lock()
	defer m.Unlock()
	return m.status
}

// WaitStopped get wait stop
func (m *defaultSchedulerWrapper[TKey, TValue]) WaitStopped(ctx context.Context, timeout time.Duration) error {
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

func (m *defaultSchedulerWrapper[TKey, TValue]) Unwrap() workerScheduler[TKey, TValue] {
	return m.worker
}

func (m *defaultSchedulerWrapper[TKey, TValue]) toStopped(err error) {
	m.status = workerStatusStopped
	m.err = err

	m.scheduleTicker.Stop()
	m.updateHeartBeatTicker.Stop()
}

func (m *defaultSchedulerWrapper[TKey, TValue]) getSQLExecutor() (se sqlexec.SQLExecutor, err error) {
	var resource pools.Resource
	if resource, err = m.sessPool.Get(); err != nil {
		return nil, err
	}

	se, ok := resource.(sqlexec.SQLExecutor)
	if !ok {
		m.sessPool.Put(resource)
		return nil, fmt.Errorf("%T cannot be casted to sqlexec.SQLExecutor", resource)
	}
	return se, nil
}

func (m *defaultSchedulerWrapper[TKey, TValue]) workerLoop() (err error) {
	var se sqlexec.SQLExecutor
	if se, err = m.getSQLExecutor(); err != nil {
		return err
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
	_ schedulerWrapper[any, any] = &nopSchedulerWrapper[any, any]{}
	_ schedulerWrapper[any, any] = &defaultSchedulerWrapper[any, any]{}
)
