package udf

import (
	"container/list"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/mctech"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

/**
 * 序列段实体。
 * 每个段内序号是连续的，由一个起始值一个结束值组成，起始值一定是小于结束值
 */
type segment struct {
	start int64
	end   int64
}

func nowUnixMilli() int64 {
	return time.Now().UnixNano() / 1000000
}

func newSegment(token string) (segment, error) {
	values := strings.SplitN(token, ",", 2)
	var s segment
	var err error
	/**
	* 序列段起始值（含）
	 */
	s.start, err = strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return s, err
	}
	/**
	* 序列段结束值（含）
	 */
	s.end, err = strconv.ParseInt(values[1], 10, 64)
	if err != nil {
		return s, err
	}
	return s, nil
}

/**
* 当前段包含的序列个数
*
* @return 当前段包含的序列个数
 */
func (s *segment) size() int64 {
	return s.end - s.start + 1
}

type segmentRange struct {
	/**
	 * 当前段，供next, nexts方法使用
	 */
	segment *segment

	/**
	 * 段内序号偏移量
	 */
	seq int64

	items []segment

	segmentIndex int

	/**
	 * 总的序列数
	 */
	size int64

	/**
	 * 已经取走的数量
	 */
	fetched int64

	expiredAt int64
}

func newSegmentRange(value string) (*segmentRange, error) {
	tokens := strings.Split(value, ";")

	var r = new(segmentRange)
	r.items = []segment{}
	var sz int64 = 0
	for _, token := range tokens {
		sg, err := newSegment(token)
		if err != nil {
			return nil, err
		}

		r.items = append(r.items, sg)
		sz += sg.size()
	}
	r.size = sz
	const cacheExpiredMs int64 = 1000
	r.expiredAt = nowUnixMilli() + cacheExpiredMs
	return r, nil
}

func (r *segmentRange) Next() int64 {
	for {
		// 已在CompositeRange中判断过，此处不再重复判断
		// if !r.HasNext() {
		// 	return -1
		// }

		if r.segment == nil {
			r.segment = &r.items[r.segmentIndex]
			r.segmentIndex++
		}

		if r.seq < r.segment.start {
			// 序列段的列表是由小到大的顺序排列的
			// seq的值小于当前段的起始值时，可直接把当前序列值设置为给定段的起始值
			r.seq = r.segment.start
			break
		}

		r.seq++
		if r.seq <= r.segment.end {
			// 下一个值仍然在当前段内部，获取成功，跳出循环
			break
		}
		// 当前段已经用完，获取下一段
		r.segment = nil
	}
	r.fetched++
	return r.seq
}

func (r *segmentRange) HasNext() bool {
	// 当前段存在且seq值小于当前段的最大值
	// 或者段迭代器后面还有值
	return (r.segment != nil && r.seq < r.segment.end) || r.segmentIndex < len(r.items)
}

func (r *segmentRange) Remaining() int64 {
	return r.size - r.fetched
}

func (r *segmentRange) Available() bool {
	return nowUnixMilli() <= r.expiredAt && r.HasNext()
}

type compositeRange struct {
	/**
	 * 当前正在使用的Range
	 */
	current *segmentRange
	/**
	 * 除current以外，剩下的已准备好，还未使用的segmentRange
	 */
	queue *list.List
	/**
	 * 队列里除了current以外剩下的序列值的数量
	 */
	remainingInQueue int64

	backendLock *sync.Mutex
	frontLock   *sync.Mutex
}

func newCompositeRange() *compositeRange {
	r := new(compositeRange)
	r.queue = list.New()
	r.remainingInQueue = 0
	r.backendLock = &sync.Mutex{}
	r.frontLock = &sync.Mutex{}
	return r
}

func (r *compositeRange) AddRange(rg *segmentRange) {
	swapped := atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&r.current)), nil, unsafe.Pointer(rg))
	if swapped {
		return
	}

	r.backendLock.Lock()
	defer r.backendLock.Unlock()
	r.queue.PushBack(rg)
	r.remainingInQueue += rg.Remaining()
}

func (r *compositeRange) Next() int64 {
	if !r.Available() {
		return -1
	}
	return r.current.Next()
}

func (r *compositeRange) Remaining() int64 {
	cur := r.current
	if cur != nil {
		return cur.Remaining() + r.remainingInQueue
	}
	return 0
}

func (r *compositeRange) Available() bool {
	cur := r.current
	if cur != nil && cur.Available() {
		return true
	}

	atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&r.current)), nil)

	r.backendLock.Lock()
	defer r.backendLock.Unlock()

	for {
		if r.queue.Len() == 0 {
			break
		}

		front := r.queue.Front()
		if front == nil {
			break
		}
		rg := front.Value.(*segmentRange)
		r.queue.Remove(front)
		r.remainingInQueue -= rg.Remaining()
		if rg.Available() {
			r.current = rg
			break
		}
	}
	return r.current != nil
}

type sequenceMetrics struct {
	Direct          int64 // 直接下载的次数
	Backend         int64 // 后台下载的次数
	TotalFetchCount int64 // 总共被取走的序列数
}

// SequenceCache sequence client(Cached)
type SequenceCache struct {
	frange *compositeRange
	// 定义后端获取序列的线程资源锁
	sem *semaphore.Weighted
	// 统计信息
	metrics sequenceMetrics
	// 服务地址前缀 'http://xxxx/'
	serviceURLPrefix string
	// 每次最大取回的序列数
	maxFetchCount int64
	// 激活后台取序列的阈值
	backendThreshold int64

	mock  bool
	debug bool
}

func newSequenceCache() *SequenceCache {
	sc := new(SequenceCache)
	option := mctech.GetOption()
	sc.frange = newCompositeRange()

	sc.mock = option.SequenceMock
	sc.debug = option.SequenceDebug

	// 后台取序列的最大线程数
	backendCount := option.SequenceBackend
	sc.maxFetchCount = option.SequenceMaxFetchCount
	sc.sem = semaphore.NewWeighted(backendCount)
	sc.serviceURLPrefix = option.SequenceAPIPrefix
	sc.backendThreshold = (sc.maxFetchCount * backendCount * 2) / 3
	return sc
}

// GetMetrics get metrics
func (s *SequenceCache) GetMetrics() *sequenceMetrics {
	return &s.metrics
}

// VersionJustPass versionJustPass
func (s *SequenceCache) VersionJustPass() (int64, error) {
	if s.mock {
		// 用于调试场景
		return 0, nil
	}

	url := s.serviceURLPrefix + "version"
	if s.debug {
		log.Debug("version just pass url", zap.String("url", url))
	}
	post, err := http.NewRequest(
		"POST", url, strings.NewReader("{ \"diff\": -3 }"))
	if err != nil {
		return 0, err
	}

	post.Header = map[string][]string{
		"Content-Type": {"application/json"},
	}

	body, err := mctech.DoRequest(post)
	if err != nil {
		return 0, err
	}

	text := string(body)
	return strconv.ParseInt(text, 10, 64)
}

// Next next
func (s *SequenceCache) Next() (int64, error) {
	if s.mock {
		// 用于调试场景
		return 0, nil
	}

	s.frange.frontLock.Lock()
	defer s.frange.frontLock.Unlock()

	value := s.frange.Next()
	var err error
	if value < 0 {
		// 没有可用的序列值，直接去服务端
		err = s.loadSequence(s.maxFetchCount)
		if err != nil {
			return -2, err
		}

		// 在锁内部可直接操作
		s.metrics.Direct++
		value = s.frange.Next()
	}

	s.backendFetchSequenceIfNeeded()
	if err != nil {
		// 在锁内部可直接操作
		return -3, err
	}
	s.metrics.TotalFetchCount++
	return value, nil
}
func (s *SequenceCache) backendFetchSequenceIfNeeded() {
	if s.frange.Remaining() > s.backendThreshold {
		return
	}

	go func() {
		if !s.sem.TryAcquire(1) {
			// 达到最大后台线程数
			return
		}

		defer s.sem.Release(1)
		err := s.loadSequence(s.maxFetchCount)
		if err != nil {
			log.Error("[backend] fetch sequence error", zap.Error(err))
		}
		atomic.AddInt64(&s.metrics.Backend, 1)
	}()
}

func (s *SequenceCache) loadSequence(count int64) error {
	url := s.serviceURLPrefix + "nexts?count=" + strconv.FormatInt(count, 10)
	post, err := http.NewRequest("POST", url, &strings.Reader{})
	if err != nil {
		return err
	}

	body, err := mctech.DoRequest(post)
	if err != nil {
		return err
	}

	text := string(body)
	rg, err := newSegmentRange(text)
	if err != nil {
		return err
	}
	s.frange.AddRange(rg)
	return nil
}

var cache *SequenceCache
var sequenceInitOnce sync.Once

// GetCache 获取带缓存的序列服务客户端
func GetCache() *SequenceCache {
	if cache != nil {
		return cache
	}

	sequenceInitOnce.Do(func() {
		cache = newSequenceCache()
		if cache.debug {
			go func() {
				c := make(chan os.Signal)
				for {
					select {
					case <-c:
						return
					default:
						start := time.Now().UnixNano()
						time.Sleep(time.Second)
						renderSequenceMetrics(start)
					}
				}
			}()
		}
		log.Info("init sequence cache")
	})
	return cache
}

func renderSequenceMetrics(startNano int64) {
	metric := cache.GetMetrics()
	if metric.TotalFetchCount == 0 {
		return
	}

	durationNano := time.Now().UnixNano() - startNano
	seconds := durationNano / int64(time.Second)
	if seconds == 0 {
		return
	}

	avgCountPerSecond := metric.TotalFetchCount / seconds
	avgFetchPerSecond := (metric.Direct + metric.Backend) / seconds
	log.Info("sequence per second metrics.",
		zap.Int64("direct", metric.Direct),
		zap.Int64("backend", metric.Backend),
		zap.Int64("avgCount", avgCountPerSecond),
		zap.Int64("avgFetch", avgFetchPerSecond),
	)
	time.Sleep(time.Second)
}
