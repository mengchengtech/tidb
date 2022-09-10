package udf

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/mctech"
	"github.com/stretchr/testify/require"
)

func TestSequence(t *testing.T) {
	now := time.Now().UnixNano()
	var rpcClient = mctech.GetRPCClient()
	mctech.SetRPCClientForTest(&mockClient{})
	defer mctech.SetRPCClientForTest(rpcClient)
	getDoFunc = createGetDoFunc("1310341421945856,1310341421945866")
	cache := GetCache()
	id, err := cache.Next()
	require.NoError(t, err)
	require.Equal(t, int64(1310341421945856), id)
	time.Sleep(time.Second)
	renderSequenceMetrics(now)
}

func TestSequenceDecodeSuccess(t *testing.T) {
	var seqID int64 = 1318030351881216
	unixTime, err := SequenceDecode(seqID)
	require.NoError(t, err)
	dt := time.UnixMilli(unixTime)
	require.Equal(t, dt.String(), "2022-07-18 10:59:52.044 +0800 CST")
}

func TestSequenceDecodeFailure(t *testing.T) {
	var seqID int64 = -30351881216
	_, err := SequenceDecode(seqID)
	require.Errorf(t, err, "Invalid negative %s specified", -30351881216)
}

func TestVersionJustPass(t *testing.T) {
	var rpcClient = mctech.GetRPCClient()
	mctech.SetRPCClientForTest(&mockClient{})
	defer mctech.SetRPCClientForTest(rpcClient)
	getDoFunc = createGetDoFunc("1310341421945866")
	cache := GetCache()
	version, err := cache.VersionJustPass()
	require.NoError(t, err)
	require.Equal(t, int64(1310341421945866), version)
}

// // 性能测试
// func _TestSequence(t *testing.T) {
// 	cache := GetCache()
// 	start := time.Now().UnixNano()
// 	go func() {
// 		for {
// 			metric := cache.GetMetrics()
// 			if metric.TotalFetchCount == 0 {
// 				continue
// 			}

// 			nano := time.Now().UnixNano() - start
// 			seconds := nano / int64(time.Second)
// 			if seconds == 0 {
// 				continue
// 			}

// 			avgCountPerSecond := metric.TotalFetchCount / seconds
// 			avgFetchPerSecond := (metric.Direct + metric.Backend) / seconds
// 			fmt.Printf("\rcount: %d, d: %d, b: %d (avg: %d, %d in %ds)\t",
// 				metric.TotalFetchCount, metric.Direct, metric.Backend,
// 				avgCountPerSecond, avgFetchPerSecond, seconds)
// 			time.Sleep(time.Second)
// 		}
// 	}()

// 	// 定义后端获取序列的线程资源锁
// 	c := make(chan os.Signal)
// 	for i := 0; i < 5; i++ {
// 		go doRun(cache)
// 	}

// 	signal := <-c
// 	fmt.Println("exit", signal)
// }

// func doRun(cache *SequenceCache) {
// 	for {
// 		_, err := cache.Next()
// 		if err != nil {
// 			fmt.Printf("Next: %s\n", err.Error())
// 			return
// 		}
// 	}
// }
