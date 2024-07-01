// add by zhangbing

package execdetails

import (
	"strconv"
	"strings"
)

const (
	MCLargeLogWriteKeysStr   = "WRITE_KEYS"
	MCLargeLogWriteSizeStr   = "WRITE_SIZE"
	MCLargeLogCopTimeStr     = "COP_TIME"
	MCLargeLogProcessTimeStr = "PROCESS_TIME"
	MCLargeLogWaitTimeStr    = "WAIT_TIME"
	MCLargeLogTotalKeysStr   = "TOTAL_KEYS"
)

// String implements the fmt.Stringer interface.
func (d ExecDetails) LargeLogString() string {
	parts := make([]string, 0, 8)
	if d.CopTime > 0 {
		parts = append(parts, MCLargeLogCopTimeStr+": "+strconv.FormatFloat(d.CopTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.ProcessTime > 0 {
		parts = append(parts, MCLargeLogProcessTimeStr+": "+strconv.FormatFloat(d.TimeDetail.ProcessTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.WaitTime > 0 {
		parts = append(parts, MCLargeLogWaitTimeStr+": "+strconv.FormatFloat(d.TimeDetail.WaitTime.Seconds(), 'f', -1, 64))
	}
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		if commitDetails.WriteKeys > 0 {
			parts = append(parts, MCLargeLogWriteKeysStr+": "+strconv.FormatInt(int64(commitDetails.WriteKeys), 10))
		}
		if commitDetails.WriteSize > 0 {
			parts = append(parts, MCLargeLogWriteSizeStr+": "+strconv.FormatInt(int64(commitDetails.WriteSize), 10))
		}
	}
	scanDetail := d.ScanDetail
	if scanDetail != nil {
		if scanDetail.TotalKeys > 0 {
			parts = append(parts, MCLargeLogTotalKeysStr+": "+strconv.FormatInt(scanDetail.TotalKeys, 10))
		}
	}
	return strings.Join(parts, " ")
}
