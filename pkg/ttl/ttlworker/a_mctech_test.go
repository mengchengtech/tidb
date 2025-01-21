// add by zhangbing

package ttlworker

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// FIXME: ttlworker里几个测试对于时间的处理不一致，写入时用的是UTC时间，读取时用的是Local时间，导致时间比较出错
	// TestTaskCancelledAfterHeartbeatTimeout
	// TestHeartBeatErrorNotBlockOthers
	// TestFinishJob
	// TestJobHeartBeatFailNotBlockOthers
	os.Setenv("TZ", "UTC")
}
