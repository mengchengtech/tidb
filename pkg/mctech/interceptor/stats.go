package interceptor

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/texttree"
	"go.uber.org/zap"
)

// ## cop
// | id                          | task              | stats                                                                                                                                                                                                                                                                                                                                                                                               |
// | ----------------------------| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
// | Sort_16                     | task:root         | stats:time:47.6s, loops:10                                                                                                                                                                                                                                                                                                                                                                          |
// | └─HashAgg_20                | task:root         | stats:[*executor.HashAggRuntimeStats] -> time:47.6s, loops:11, partial_worker:{wall_time:47.58565987s, concurrency:5, task_num:35, tot_wait:3m57.738882619s, tot_exec:181.14016ms, tot_time:3m57.925381959s, max:47.585596374s, p95:47.585596374s}, final_worker:{wall_time:47.595431093s, concurrency:5, task_num:25, tot_wait:3m57.925514047s, tot_exec:50.865569ms, tot_time:3m57.976393245s, max:47.595376811s, p95:47.595376811s} |
// |   └─HashJoin_25             | task:root         | stats:[*executor.hashJoinRuntimeStats] -> time:47.6s, loops:36, build_hash_table:{total:80.9ms, fetch:80ms, build:891.7µs}, probe:{concurrency:5, total:3m48.5s, max:47.6s, probe:3m47s, fetch:1.42s}                                                                                                                                                                                                                                   |
// |     ├─TableReader_31(Build) | task:root         | stats:[*distsql.selectResultRuntimeStats] -> time:79.9ms, loops:10, cop_task: {num: 8, max: 81.3ms, min: 21.3ms, avg: 34ms, p95: 81.3ms, rpc_num: 8, rpc_time: 271.6ms, copr_cache_hit_ratio: 0.00, build_task_duration: 3.69ms, max_distsql_concurrency: 8}                                                                                                                                                                               |
// |     │ └─Selection_30        | task:cop[tiflash] | stats:tiflash_task:{proc max:52.5ms, min:6.64ms, avg: 24.8ms, p80:44.5ms, p95:52.5ms, iters:3, tasks:8, threads:8}                                                                                                                                                                                                                                                                                  |
// |     │   └─TableFullScan_29  | task:cop[tiflash] | stats:tiflash_task:{proc max:44.5ms, min:6.64ms, avg: 23.8ms, p80:44.5ms, p95:44.5ms, iters:3, tasks:8, threads:8}, tiflash_scan:{dtfile:{total_scanned_packs:13, total_skipped_packs:827, total_scanned_rows:106496, total_skipped_rows:6678966, total_rs_index_load_time: 8ms, total_read_time: 25ms}, total_create_snapshot_time: 0ms, total_local_region_num: 0, total_remote_region_num: 0}    |
// |     └─TableReader_41(Probe) | task:root         | stats:[*distsql.selectResultRuntimeStats] -> time:284.8ms, loops:25, cop_task: {num: 8, max: 283.1ms, min: 21.8ms, avg: 57.5ms, p95: 283.1ms, rpc_num: 8, rpc_time: 459.9ms, copr_cache_hit_ratio: 0.00, build_task_duration: 50.7µs, max_distsql_concurrency: 8}                                                                                                                                                                          |
// |       └─Selection_40        | task:cop[tiflash] | stats:tiflash_task:{proc max:104.1ms, min:4.88ms, avg: 25.7ms, p80:20ms, p95:104.1ms, iters:3, tasks:8, threads:8}                                                                                                                                                                                                                                                                                  |
// |         └─TableFullScan_39  | task:cop[tiflash] | stats:tiflash_task:{proc max:96.1ms, min:4.88ms, avg: 24.7ms, p80:20ms, p95:96.1ms, iters:3, tasks:8, threads:8}, tiflash_scan:{dtfile:{total_scanned_packs:13, total_skipped_packs:827, total_scanned_rows:106496, total_skipped_rows:6678966, total_rs_index_load_time: 0ms, total_read_time: 79ms}, total_create_snapshot_time: 0ms, total_local_region_num: 0, total_remote_region_num: 0}      |
// {"all":"47.63671446s","parse":"0s","plan":"4.346939ms","cop":"361.911791ms","ready":"47.620866538s","send":"15.847922ms"}

// ## tikv
// | id                           | task           | stats                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
// | ---------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
// | Sort_16                          | task:root      | stats:time:48.9s, loops:10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
// | └─HashAgg_18                     | task:root      | stats:time:[*executor.HashAggRuntimeStats] -> 48.9s, loops:11, partial_worker:{wall_time:48.876645541s, concurrency:5, task_num:36, tot_wait:4m4.214411309s, tot_exec:157.799667ms, tot_time:4m4.377739701s, max:48.876575859s, p95:48.876575859s}, final_worker:{wall_time:48.886994145s, concurrency:5, task_num:25, tot_wait:4m4.37971012s, tot_exec:53.246267ms, tot_time:4m4.432969642s, max:48.886911045s, p95:48.886911045s}                                                                                                                                                                                   |
// |   └─HashJoin_20                  | task:root      | stats:time:[*executor.hashJoinRuntimeStats] -> 48.9s, loops:37, build_hash_table:{total:45.8ms, fetch:44.6ms, build:1.25ms}, probe:{concurrency:5, total:3m50.2s, max:48.9s, probe:3m49.9s, fetch:285.1ms}                                                                                                                                                                                                                                                                                                                                                                                                             |
// |     ├─IndexLookUp_27(Build)      | task:root      | stats:[*executor.IndexLookUpRunTimeStats] -> time:44.1ms, loops:10, index_task: {total_time: 26.9ms, fetch_handle: 26.8ms, build: 9.99µs, wait: 54.3µs}, table_task: {total_time: 37.1ms, num: 7, concurrency: 5}, next: {wait_index: 16.6ms, wait_table_lookup_build: 9.86ms, wait_table_lookup_resp: 12.1ms}                                                                                                                                                                                                                                                                                                            |
// |     │ ├─IndexRangeScan_24(Build) | task:cop[tikv] | stats:[*distsql.selectResultRuntimeStats] -> time:11ms, loops:58, cop_task: {num: 9, max: 3.38ms, min: 576.2µs, avg: 1.47ms, p95: 3.38ms, max_proc_keys: 992, p95_proc_keys: 992, tot_proc: 2ms, tot_wait: 1.65ms, rpc_num: 9, rpc_time: 13.1ms, copr_cache_hit_ratio: 0.67, build_task_duration: 1.16ms, max_distsql_concurrency: 1}, tikv_task:{proc max:12ms, min:0s, avg: 4ms, p80:12ms, p95:12ms, iters:90, tasks:9}, scan_detail: {total_process_keys: 1696, total_process_keys_size: 167904, total_keys: 1699, get_snapshot_time: 1.2ms, rocksdb: {key_skipped_count: 1696, block: {cache_hit_count: 13}           |
// |     │ └─Selection_26(Probe)      | task:cop[tikv] | stats:[*distsql.selectResultRuntimeStats] -> time:20.2ms, loops:19, cop_task: {num: 7, max: 5.52ms, min: 333.1µs, avg: 2.31ms, p95: 5.52ms, max_proc_keys: 1024, p95_proc_keys: 1024, tot_proc: 5.28ms, tot_wait: 6.16ms, rpc_num: 7, rpc_time: 16ms, copr_cache_hit_ratio: 0.71, build_task_duration: 726.2µs, max_distsql_concurrency: 1}, tikv_task:{proc max:44ms, min:0s, avg: 18.9ms, p80:40ms, p95:44ms, iters:88, tasks:7}, scan_detail: {total_process_keys: 1836, total_process_keys_size: 798083, total_keys: 1838, get_snapshot_time: 5.87ms, rocksdb: {key_skipped_count: 3609, block: {cache_hit_count: 45} |
// |     │   └─TableRowIDScan_25      | task:cop[tikv] | stats:tikv_task:{proc max:36ms, min:0s, avg: 16.6ms, p80:32ms, p95:36ms, iters:88, tasks:7}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
// |     └─IndexLookUp_34(Probe)      | task:root      | stats:[*executor.IndexLookUpRunTimeStats] -> time:60.5ms, loops:25, index_task: {total_time: 14.8ms, fetch_handle: 14.8ms, build: 9.47µs, wait: 44.7µs}, table_task: {total_time: 57.8ms, num: 7, concurrency: 5}, next: {wait_index: 7.67ms, wait_table_lookup_build: 2.76ms, wait_table_lookup_resp: 9.15ms}                                                                                                                                                                                                                                                                                                            |
// |       ├─IndexRangeScan_31(Build) | task:cop[tikv] | stats:[*distsql.selectResultRuntimeStats] -> time:9.69ms, loops:58, cop_task: {num: 9, max: 2.92ms, min: 548.6µs, avg: 1.31ms, p95: 2.92ms, max_proc_keys: 992, p95_proc_keys: 992, tot_proc: 2.09ms, tot_wait: 1.7ms, rpc_num: 9, rpc_time: 11.6ms, copr_cache_hit_ratio: 0.67, build_task_duration: 1.07ms, max_distsql_concurrency: 1}, tikv_task:{proc max:12ms, min:0s, avg: 4ms, p80:12ms, p95:12ms, iters:90, tasks:9}, scan_detail: {total_process_keys: 1696, total_process_keys_size: 167904, total_keys: 1699, get_snapshot_time: 1.21ms, rocksdb: {key_skipped_count: 1696, block: {cache_hit_count: 13}      |
// |       └─Selection_33(Probe)      | task:cop[tikv] | stats:[*distsql.selectResultRuntimeStats] -> time:41.8ms, loops:31, cop_task: {num: 7, max: 6.07ms, min: 367.6µs, avg: 2.81ms, p95: 6.07ms, max_proc_keys: 812, p95_proc_keys: 812, tot_proc: 3.62ms, tot_wait: 7.4ms, rpc_num: 7, rpc_time: 19.5ms, copr_cache_hit_ratio: 0.86, build_task_duration: 626.5µs, max_distsql_concurrency: 1}, tikv_task:{proc max:76ms, min:4ms, avg: 25.1ms, p80:44ms, p95:76ms, iters:88, tasks:7}, scan_detail: {total_process_keys: 812, total_process_keys_size: 333647, total_keys: 813, get_snapshot_time: 6.97ms, rocksdb: {key_skipped_count: 1563, block: {cache_hit_count: 20}   |
// |         └─TableRowIDScan_32      | task:cop[tikv] | stats:tikv_task:{proc max:76ms, min:4ms, avg: 24ms, p80:40ms, p95:76ms, iters:88, tasks:7}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
// {"time":{"all":"48.924997165s","parse":"0s","plan":"2.581943ms","cop":"79.810629ms","ready":"48.908635656s","send":"16.361509ms"}

type planData struct {
	tidbTime       time.Duration
	tikvCopTime    time.Duration
	tiflashCopTime time.Duration
	maxActRows     int64
}

type planStatCollector struct {
	renderDebug bool
	stmtCtx     *stmtctx.StatementContext
	statRecords []*shortRuntimeStatsRecord
}

func newPlanStatCollector(stmtCtx *stmtctx.StatementContext) *planStatCollector {
	return &planStatCollector{
		renderDebug: false,
		stmtCtx:     stmtCtx,
	}
}

// collect 获取sql执行算子的执行时间信息
func (c *planStatCollector) collect() (ct planData) {
	// 捕获后续执行的异常，不再向外抛出
	defer func() {
		if err := recover(); err != nil {
			logutil.BgLogger().Warn("[collectCPUTime] 获取cpu运行时间出错", zap.Error(err.(error)), zap.Stack("stack"))
		}
	}()

	c.collectFromFlatPlan()

	var rows []string
	for _, r := range c.statRecords {
		if c.renderDebug {
			rows = append(rows, r.String())
		}
		details := r.GetDetails()
		if ct.maxActRows < details.actRows {
			ct.maxActRows = details.actRows
		}
		if r.isRoot {
			// root task，在 tidb-server 上执行
			// 非 root task(cop task)，在 TiKV 或者 TiFlash 上并行执行
			// 由于从现有的统计信息里不能方便的收集到tidb-server上消耗的时间，因此在这里从执行计划中单独获取
			ct.tidbTime = ct.tidbTime + details.rootProcTime
		} else {
			copProcTime := time.Duration(details.copProcTimes.Sum())
			switch r.storeType {
			case kv.TiKV:
				ct.tikvCopTime = ct.tikvCopTime + copProcTime
			case kv.TiFlash:
				ct.tiflashCopTime = ct.tiflashCopTime + copProcTime
			}
		}
	}
	if c.renderDebug {
		logutil.BgLogger().Warn("object inspect", zap.Duration("tidbTime", ct.tidbTime),
			zap.Duration("tikvCopTime", ct.tikvCopTime), zap.Duration("tiflashCopTime", ct.tiflashCopTime),
			zap.Int64("maxActRows", ct.maxActRows),
			zap.String("explain", strings.Join(rows, "\n")))

		plan, _ := executor.GetEncodedPlan(c.stmtCtx, false)
		logutil.BgLogger().Warn("encode plan", zap.String("plan", fmt.Sprintf("tidb_encode_plan('%s')", plan)))
	}
	return ct
}

func (c *planStatCollector) collectFromFlatPlan() {
	flat := executor.GetFlatPlan(c.stmtCtx)
	if flat == nil || len(flat.Main) == 0 {
		return
	}
	// flat.Main[0] must be the root node of tree
	c.encodeFlatPlanTree(flat.Main, 0)

	for _, cte := range flat.CTEs {
		c.collectFromFlatOp(cte[0])
		if len(cte) > 1 {
			c.encodeFlatPlanTree(cte[1:], 1)
		}
	}
}

func (c *planStatCollector) encodeFlatPlanTree(flatTree core.FlatPlanTree, offset int) {
	for i := 0; i < len(flatTree); {
		op := flatTree[i]
		c.collectFromFlatOp(op)
		if op.NeedReverseDriverSide {
			buildSide := flatTree[op.ChildrenIdx[1]-offset : op.ChildrenEndIdx+1-offset]
			probeSide := flatTree[op.ChildrenIdx[0]-offset : op.ChildrenIdx[1]-offset]
			c.encodeFlatPlanTree(buildSide, op.ChildrenIdx[1])
			c.encodeFlatPlanTree(probeSide, op.ChildrenIdx[0])
			// Skip the children plan tree of the current operator.
			i = op.ChildrenEndIdx + 1 - offset
		} else {
			i++
		}
	}
}

func (c *planStatCollector) collectFromFlatOp(flatOp *core.FlatOperator) {
	var taskType string
	if flatOp.IsRoot {
		taskType = "root"
	} else {
		taskType = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	p := flatOp.Origin

	// "executeInfo": "time:48.2s, loops:37, build_hash_table:{total:131ms, fetch:129.8ms, build:1.17ms}, probe:{concurrency:5, total:3m49.6s, max:48.2s, probe:3m48.7s, fetch:846ms}",
	// 输出 execute info
	// 其中 basic 表示主时间轴的时间信息，groups 表示各种详细时间信息
	var (
		rootStats *execdetails.RootRuntimeStats
		copStats  *execdetails.CopRuntimeStats
	)

	sctx := p.SCtx()
	runtimeStatsColl := sctx.GetSessionVars().StmtCtx.RuntimeStatsColl
	if runtimeStatsColl == nil {
		return
	}

	explainID := p.ID()
	if runtimeStatsColl.ExistsRootStats(explainID) {
		rootStats = runtimeStatsColl.GetRootStats(explainID)
	}
	if runtimeStatsColl.ExistsCopStats(explainID) {
		copStats = runtimeStatsColl.GetCopStats(explainID)
	}

	var textTreeExplainID string
	if c.renderDebug {
		explainID := p.ExplainID().String() + flatOp.Label.String()
		textTreeExplainID = texttree.PrettyIdentifier(explainID, flatOp.TextTreeIndent, flatOp.IsLastChild)
	}

	record := &shortRuntimeStatsRecord{
		id:        textTreeExplainID,
		isRoot:    flatOp.IsRoot,
		taskType:  taskType,
		storeType: flatOp.StoreType,
		rootStats: rootStats,
		copStats:  copStats,
	}
	c.statRecords = append(c.statRecords, record)
}

type shortRuntimeStatsRecord struct {
	id        string
	isRoot    bool
	taskType  string
	storeType kv.StoreType

	rootStats *execdetails.RootRuntimeStats
	copStats  *execdetails.CopRuntimeStats

	details *shortRuntimeStatsDetails
}

type shortRuntimeStatsDetails struct {
	rootProcTime    time.Duration
	actRows         int64
	copProcTimes    execdetails.Percentile[execdetails.Duration]
	copTotalTime    time.Duration
	copTotalTasks   int32
	copTotalLoops   int32
	copTotalThreads int32
}

func (s *shortRuntimeStatsRecord) GetDetails() *shortRuntimeStatsDetails {
	if s.details != nil {
		return s.details
	}

	// "executeInfo": "time:48.2s, loops:37, build_hash_table:{total:131ms, fetch:129.8ms, build:1.17ms}, probe:{concurrency:5, total:3m49.6s, max:48.2s, probe:3m48.7s, fetch:846ms}",
	// 其中 _ 表示主时间轴的时间信息，groups 表示各种详细时间信息
	var (
		rootProcTime = time.Duration(0)
		actRows      = int64(0)
	)
	if s.rootStats != nil {
		_, groups := s.rootStats.MergeStats()
		for _, group := range groups {
			if collector, ok := group.(mctech.CPUTimeCollector); ok {
				stats := collector.Collect()
				if stats.Group == mctech.Root {
					rootProcTime = rootProcTime + stats.Time
				}
			}
		}
		actRows = s.rootStats.GetActRows()
	}

	// "executeInfo": "time:79.9ms, loops:10, cop_task: {num: 8, max: 81.3ms, min: 21.3ms, avg: 34ms, p95: 81.3ms, rpc_num: 8, rpc_time: 271.6ms, copr_cache_hit_ratio: 0.00, build_task_duration: 3.69ms, max_distsql_concurrency: 8}
	d := &shortRuntimeStatsDetails{
		rootProcTime: rootProcTime,
		actRows:      actRows,
	}
	s.details = d
	if s.copStats != nil {
		d.actRows = s.copStats.GetActRows()
		if s.copStats.HasStats() {
			d.copProcTimes, d.copTotalTime, d.copTotalTasks, d.copTotalLoops, d.copTotalThreads, _ = s.copStats.MergeBasicStats()
		}
	}
	return d
}

func (s *shortRuntimeStatsRecord) String() string {
	details := s.GetDetails()
	return fmt.Sprintf("%v | actRows: %d, task:%s | stats:%s", s.id, details.actRows, s.taskType, s.StatString())
}

func (s *shortRuntimeStatsRecord) StatString() string {
	analyzeInfo := ""
	var tps []string
	if s.rootStats != nil {
		analyzeInfo = "[" + reflect.TypeOf(s.rootStats).String() + "] =>"
		// "executeInfo": "time:48.2s, loops:37, build_hash_table:{total:131ms, fetch:129.8ms, build:1.17ms}, probe:{concurrency:5, total:3m49.6s, max:48.2s, probe:3m48.7s, fetch:846ms}",
		// 输出 execute info
		// 其中 basic 表示主时间轴的时间信息，groups 表示各种详细时间信息
		_, groups := s.rootStats.MergeStats()
		for _, group := range groups {
			tps = append(tps, reflect.TypeOf(group).String())
		}
		analyzeInfo = analyzeInfo + s.rootStats.String()
	}
	if s.copStats != nil {
		if len(analyzeInfo) > 0 {
			analyzeInfo += ", "
		}
		analyzeInfo += "[copStats]: " + s.copStats.String()
	}
	if len(tps) > 0 {
		analyzeInfo = "[" + strings.Join(tps, ",") + "] -> " + analyzeInfo
	}
	return analyzeInfo
}

// RuntimeStatsWithConcurrencyInfo
// HashAggRuntimeStats