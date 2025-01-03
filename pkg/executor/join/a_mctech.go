// add by zhangbing

package join

import (
	"time"

	"github.com/pingcap/tidb/pkg/mctech"
)

// Collect collect cpu time
func (e *hashJoinRuntimeStats) Collect() *mctech.CPUTimeStats {
	var cpuTime time.Duration
	if e.hashStat.buildTableElapse > 0 {
		cpuTime = e.hashStat.buildTableElapse
	}
	if e.probe > 0 {
		cpuTime = cpuTime + time.Duration(e.probe)
	}
	return &mctech.CPUTimeStats{
		Group: mctech.Root,
		Type:  "HashJoin",
		Time:  cpuTime,
	}
}

// Collect collect cpu time
func (e *indexLookUpJoinRuntimeStats) Collect() *mctech.CPUTimeStats {
	var cpuTime int64
	if e.innerWorker.totalTime > 0 {
		cpuTime = e.innerWorker.totalTime
	}
	if e.probe > 0 {
		// index join
		cpuTime = cpuTime + e.probe
	}
	if e.innerWorker.join > 0 {
		// index hash join
		cpuTime = cpuTime + e.innerWorker.join
	}
	return &mctech.CPUTimeStats{
		Group: mctech.Root,
		Type:  "IndexLookUpJoin",
		Time:  time.Duration(cpuTime),
	}
}

var (
	_ mctech.CPUTimeCollector = &hashJoinRuntimeStats{}
	_ mctech.CPUTimeCollector = &indexLookUpJoinRuntimeStats{}
)
