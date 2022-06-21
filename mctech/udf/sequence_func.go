package udf

import (
	"container/list"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type IRange interface {
	Next() (int64, error)
	HasNext() bool
	Remaining() int64
	Available() bool
}

/**
 * 序列段实体。
 * 每个段内序号是连续的，由一个起始值一个结束值组成，起始值一定是小于结束值
 *
 * @author zhangbing
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
	const CACHE_EXPIRED_MS int64 = 1000
	r.expiredAt = nowUnixMilli() + CACHE_EXPIRED_MS
	return r, nil
}

func (r *segmentRange) Next() (int64, error) {
	for {
		if !r.HasNext() {
			return 0, fmt.Errorf("返回的序列值已用完")
		}

		if r.segment == nil {
			r.segment = &r.items[r.segmentIndex]
			r.segmentIndex++
		}

		if r.seq < r.segment.start {
			// 序列段的列表是由小到大的顺序排列的
			// seq的值小于当前段的起始值时，可直接把当前序列值设置为给定段的起始值
			r.seq = r.segment.start
			break
		} else {
			r.seq++
			if r.seq <= r.segment.end {
				// 下一个值仍然在当前段内部，获取成功，跳出循环
				break
			} else {
				// 当前段已经用完，获取下一段
				r.segment = nil
			}
		}
	}
	r.fetched++
	return r.seq, nil
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
	current IRange
	/**
	 * 除current以外，剩下的已准备好，还未使用的IRange
	 */
	queue *list.List
	/**
	 * 队列里除了current以外剩下的序列值的数量
	 */
	remainingInQueue int64

	lock *sync.Mutex
}

func newCompositeRange() *compositeRange {
	r := new(compositeRange)
	r.queue = list.New()
	r.remainingInQueue = 0
	r.lock = &sync.Mutex{}
	return r
}

func (r *compositeRange) AddRange(rg IRange) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.current == nil {
		r.current = rg
	} else {
		r.queue.PushBack(rg)
		r.remainingInQueue += rg.Remaining()
	}
}

func (r *compositeRange) Next() (int64, error) {
	if !r.Available() {
		return -1, fmt.Errorf("返回的序列值已用完")
	}
	return r.current.Next()
}

func (r *compositeRange) HasNext() bool {
	// 只要 remainingInQueue 有值，表示除current以外还有待取的值，返回true
	if r.remainingInQueue > 0 {
		return true
	}

	// remainingInQueue 为0时，再来检查current
	rg := r.current
	if rg == nil {
		return false
	}

	return rg.HasNext()
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

	r.lock.Lock()
	defer r.lock.Unlock()
	r.current = nil

	for {
		if r.queue.Len() == 0 {
			break
		}

		front := r.queue.Front()
		rg := front.Value.(IRange)
		r.queue.Remove(front)
		r.remainingInQueue -= rg.Remaining()
		if rg.Available() {
			r.current = rg
			break
		}
	}
	return r.current != nil
}

type SequenceMetrics struct {
	Direct          int64 // 直接下载的次数
	Backend         int64 // 后台下载的次数
	TotalFetchCount int64 // 总共被取走的序列数
}

type SequenceCache struct {
	frange *compositeRange
	// 定义后端获取序列的线程资源锁
	sem *semaphore.Weighted
	// 统计信息
	metrics SequenceMetrics
	// 服务地址前缀 'http://xxxx/'
	serviceUrlPrefix string
	// 每次最大取回的序列数
	maxFetchCount int64
	// 激活后台取序列的阈值
	backendThreshold int64

	mock bool
}

func newSequenceCache() *SequenceCache {
	sc := new(SequenceCache)
	sc.frange = newCompositeRange()

	sc.mock = option.getMock()

	// 后台取序列的最大线程数
	backendCount := option.getBackendCount()
	sc.maxFetchCount = option.getMaxFetchCount()
	sc.sem = semaphore.NewWeighted(backendCount)
	sc.serviceUrlPrefix = option.getSequenceServiceUrlPrefix()
	sc.backendThreshold = (sc.maxFetchCount * backendCount * 2) / 3
	return sc
}

func (s *SequenceCache) GetMetrics() *SequenceMetrics {
	return &s.metrics
}

func (s *SequenceCache) Next() (int64, error) {
	if s.mock {
		// 用于调试场景
		return 0, nil
	}

	// 数据是否可用
	available := s.frange.Available()
	var err error
	if !available {
		// 数据不可用，直接再取一次
		rg, err := s.loadSequence(s.maxFetchCount)
		if err != nil {
			return -1, err
		}
		s.metrics.Direct++
		s.frange.AddRange(rg)
	}

	value, err := s.frange.Next()
	s.backendFetchSequenceIfNeeded()
	s.metrics.TotalFetchCount++
	return value, err
}

func (s *SequenceCache) VersionJustPass() (int64, error) {
	if s.mock {
		// 用于调试场景
		return 0, nil
	}

	url := s.serviceUrlPrefix + "version"
	log.Debug("version just pass url", zap.String("url", url))
	post, err := http.NewRequest(
		"POST", url, strings.NewReader("{ \"diff\": -3 }"))
	if err != nil {
		return 0, err
	}

	post.Header = map[string][]string{
		"Content-Type": {"application/json"},
	}

	body, err := DoRequest(post)
	if err != nil {
		return 0, err
	}

	text := string(body)
	return strconv.ParseInt(text, 10, 64)
}

func (s *SequenceCache) loadSequence(count int64) (*segmentRange, error) {
	url := s.serviceUrlPrefix + "nexts?count=" + strconv.FormatInt(count, 10)
	log.Debug("next sequence url", zap.String("url", url))
	post, err := http.NewRequest("POST", url, &strings.Reader{})
	if err != nil {
		return nil, err
	}

	body, err := DoRequest(post)
	if err != nil {
		return nil, err
	}

	text := string(body)
	return newSegmentRange(text)
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

		// fmt.Println("=====================load from backend")
		var err error
		rg, err := s.loadSequence(s.maxFetchCount)
		if err != nil {
			panic(err)
		}
		s.metrics.Backend++
		s.frange.AddRange(rg)
	}()
}

var cache *SequenceCache

func GetCache() *SequenceCache {
	if cache == nil {
		cache = newSequenceCache()
	}
	log.Debug("get sequence cache")
	return cache
}
