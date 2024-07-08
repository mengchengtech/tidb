// add by zhangbing

package execdetails

import (
	"strconv"
	"strings"
)

const (
	// MCLargeQueryWriteKeysStr means the count of keys in the transaction.
	MCLargeQueryWriteKeysStr = "WRITE_KEYS"
	// MCLargeQueryWriteSizeStr means the key/value size in the transaction.
	MCLargeQueryWriteSizeStr = "WRITE_SIZE"
	// MCLargeQueryCopTimeStr represents the sum of cop-task time spend in TiDB distSQL.
	MCLargeQueryCopTimeStr = "COP_TIME"
	// MCLargeQueryProcessTimeStr represents the sum of process time of all the coprocessor tasks.
	MCLargeQueryProcessTimeStr = "PROCESS_TIME"
	// MCLargeQueryWaitTimeStr means the time of all coprocessor wait.
	MCLargeQueryWaitTimeStr = "WAIT_TIME"
	// MCLargeQueryTotalKeysStr means the total scan keys.
	MCLargeQueryTotalKeysStr = "TOTAL_KEYS"
)

// LargeQueryString use for Large SQL log recording.
func (d ExecDetails) LargeQueryString() string {
	parts := make([]string, 0, 8)
	if d.CopTime > 0 {
		parts = append(parts, MCLargeQueryCopTimeStr+": "+strconv.FormatFloat(d.CopTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.ProcessTime > 0 {
		parts = append(parts, MCLargeQueryProcessTimeStr+": "+strconv.FormatFloat(d.TimeDetail.ProcessTime.Seconds(), 'f', -1, 64))
	}
	if d.TimeDetail.WaitTime > 0 {
		parts = append(parts, MCLargeQueryWaitTimeStr+": "+strconv.FormatFloat(d.TimeDetail.WaitTime.Seconds(), 'f', -1, 64))
	}
	commitDetails := d.CommitDetail
	if commitDetails != nil {
		if commitDetails.WriteKeys > 0 {
			parts = append(parts, MCLargeQueryWriteKeysStr+": "+strconv.FormatInt(int64(commitDetails.WriteKeys), 10))
		}
		if commitDetails.WriteSize > 0 {
			parts = append(parts, MCLargeQueryWriteSizeStr+": "+strconv.FormatInt(int64(commitDetails.WriteSize), 10))
		}
	}
	scanDetail := d.ScanDetail
	if scanDetail != nil {
		if scanDetail.TotalKeys > 0 {
			parts = append(parts, MCLargeQueryTotalKeysStr+": "+strconv.FormatInt(scanDetail.TotalKeys, 10))
		}
	}
	return strings.Join(parts, " ")
}
