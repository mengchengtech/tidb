package udf

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// 性能测试
func _TestSequence(t *testing.T) {
	cache := GetCache()
	start := time.Now().UnixNano()
	go func() {
		for {
			metric := cache.GetMetrics()
			if metric.TotalFetchCount == 0 {
				continue
			}

			nano := time.Now().UnixNano() - start
			seconds := nano / int64(time.Second)
			if seconds == 0 {
				continue
			}

			avgCountPerSecond := metric.TotalFetchCount / seconds
			avgFetchPerSecond := (metric.Direct + metric.Backend) / seconds
			fmt.Printf("\rcount: %d, d: %d, b: %d (avg: %d, %d in %ds)\t",
				metric.TotalFetchCount, metric.Direct, metric.Backend,
				avgCountPerSecond, avgFetchPerSecond, seconds)
			time.Sleep(time.Second)
		}
	}()

	// 定义后端获取序列的线程资源锁
	c := make(chan os.Signal)
	for i := 0; i < 5; i++ {
		go doRun(cache)
	}

	signal := <-c
	fmt.Println("exit", signal)
}

func doRun(cache *SequenceCache) {
	for {
		_, err := cache.Next()
		if err != nil {
			fmt.Printf("Next: %s\n", err.Error())
			return
		}
	}
}
