// add by zhangbing

package aggregate

import (
	"time"

	"github.com/pingcap/tidb/pkg/mctech"
)

// Collect collect cpu time
func (e *HashAggRuntimeStats) Collect() *mctech.CPUTimeStats {
	var cpuTime int64
	for _, partial := range e.PartialStats {
		cpuTime = cpuTime + partial.ExecTime
	}

	for _, final := range e.FinalStats {
		cpuTime = cpuTime + final.ExecTime
	}
	return &mctech.CPUTimeStats{
		Group: mctech.Root,
		Type:  "HashAggregate",
		Time:  time.Duration(cpuTime),
	}
}

var (
	_ mctech.CPUTimeCollector = &HashAggRuntimeStats{}
)
