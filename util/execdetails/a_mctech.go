// add by zhangbing

package execdetails

import (
	"strconv"
	"strings"
)

const (
	MCTechWriteKeysStr   = "WRITE_KEYS"
	MCTechWriteSizeStr   = "WRITE_SIZE"
	MCTechCopTimeStr     = "COP_TIME"
	MCTechProcessTimeStr = "PROCESS_TIME"
	MCTechWaitTimeStr    = "WAIT_TIME"
	MCTechTotalKeysStr   = "TOTAL_KEYS"
)

// String implements the fmt.Stringer interface.
func (d ExecDetails) LargeQueryString() string {
	parts := make([]string, 0, 8)
	if d.CopTime > 0 {
		parts = append(parts, MCTechCopTimeStr+": "+strconv.FormatFloat(d.CopTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.ProcessTime > 0 {
		parts = append(parts, MCTechProcessTimeStr+": "+strconv.FormatFloat(d.TimeDetail.ProcessTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.WaitTime > 0 {
		parts = append(parts, MCTechWaitTimeStr+": "+strconv.FormatFloat(d.TimeDetail.WaitTime.Seconds(), 'f', -1, 64))
	}
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		if commitDetails.WriteKeys > 0 {
			parts = append(parts, MCTechWriteKeysStr+": "+strconv.FormatInt(int64(commitDetails.WriteKeys), 10))
		}
		if commitDetails.WriteSize > 0 {
			parts = append(parts, MCTechWriteSizeStr+": "+strconv.FormatInt(int64(commitDetails.WriteSize), 10))
		}
	}
	scanDetail := d.ScanDetail
	if scanDetail != nil {
		if scanDetail.TotalKeys > 0 {
			parts = append(parts, MCTechTotalKeysStr+": "+strconv.FormatInt(scanDetail.TotalKeys, 10))
		}
	}
	return strings.Join(parts, " ")
}
