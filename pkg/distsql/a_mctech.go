// add by zhangbing

package distsql

import (
	"time"

	"github.com/pingcap/tidb/pkg/mctech"
)

// Collect collect cpu time
func (e *selectResultRuntimeStats) Collect() *mctech.CPUTimeStats {
	var cpuTime time.Duration = 0
	if e.totalProcessTime > 0 {
		cpuTime = cpuTime + e.totalProcessTime
	}
	return &mctech.CPUTimeStats{
		Group: mctech.Cop,
		Type:  "SelectResult",
		Time:  cpuTime,
	}
}

var (
	_ mctech.CPUTimeCollector = &selectResultRuntimeStats{}
)
