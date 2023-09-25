// add by zhangbing

package execdetails

import (
	"strconv"
	"strings"
)

const (
	// MCLargeLogWriteKeysStr means the count of keys in the transaction.
	MCLargeLogWriteKeysStr = "WRITE_KEYS"
	// MCLargeLogWriteSizeStr means the key/value size in the transaction.
	MCLargeLogWriteSizeStr = "WRITE_SIZE"
	// MCLargeLogCopTimeStr represents the sum of cop-task time spend in TiDB distSQL.
	MCLargeLogCopTimeStr = "COP_TIME"
	// MCLargeLogProcessTimeStr represents the sum of process time of all the coprocessor tasks.
	MCLargeLogProcessTimeStr = "PROCESS_TIME"
	// MCLargeLogWaitTimeStr means the time of all coprocessor wait.
	MCLargeLogWaitTimeStr = "WAIT_TIME"
	// MCLargeLogTotalKeysStr means the total scan keys.
	MCLargeLogTotalKeysStr = "TOTAL_KEYS"
)

// LargeLogString use for Large SQL log recording.
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
