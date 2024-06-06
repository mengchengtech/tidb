package mctech

import (
	"time"
)

// TaskType task type
type TaskType = int

const (
	// Root root task
	Root TaskType = 1 << iota
	// Cop cop task
	Cop
)

// CPUTimeStats cpu time stats
type CPUTimeStats struct {
	Group TaskType
	Type  string
	Time  time.Duration
}

// CPUTimeCollector collector interface that collect cpu time
type CPUTimeCollector interface {
	// Collect 获取执行sql消耗的cpu时间。目前只返回了运行在tidb上的算子消耗的除cop任务以外的时间
	Collect() *CPUTimeStats
}
