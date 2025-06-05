package interceptor_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech/interceptor"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// | id | estRows | estCost | actRows | task | access object | execution info | operator info | memory  | disk |

// | id                               | estRows  | estCost     | actRows | task      | access object                                                                             | execution info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | operator info                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | memory  | disk     |
// | Sort_16                          | 77.78    | 2312979.73  | 8421    | root      |                                                                                           | time:1m47.2s, loops:10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | global_ec3.project_construction_quantity_contract_bill_part.level:desc, global_ec3.project_construction_quantity_contract_bill_part.order_no                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | 3.48 MB | 0 Bytes  |
// | └─HashAgg_20                     | 77.78    | 2311513.94  | 8421    | root      |                                                                                           | time:1m47.2s, loops:17, partial_worker:{wall_time:1m47.17484498s, concurrency:16, task_num:38, tot_wait:28m33.683430141s, tot_exec:272.592059ms, tot_time:28m33.95901246s, max:1m47.124546249s, p95:1m47.124546249s}, final_worker:{wall_time:1m47.236099362s, concurrency:16, task_num:231, tot_wait:28m34.899401745s, tot_exec:96.339042ms, tot_time:28m34.995796917s, max:1m47.21733377s, p95:1m47.21733377s}                                                                                                                                                                         | group by:global_ec3.project_construction_quantity_contract_bill_part.construction_quantity_id, global_ec3.project_construction_quantity_contract_bill_part.current_bill_quantity, global_ec3.project_construction_quantity_contract_bill_part.entry_work_id, global_ec3.project_construction_quantity_contract_bill_part.entry_work_name, global_ec3.project_construction_quantity_contract_bill_part.id, global_ec3.project_construction_quantity_contract_bill_part.is_leaf, global_ec3.project_construction_quantity_contract_bill_part.level, global_ec3.project_construction_quantity_contract_bill_part.order_no, global_ec3.project_construction_quantity_contract_bill_part.org_id, global_ec3.project_construction_quantity_contract_bill_part.parent_id, global_ec3.project_construction_quantity_contract_bill_part.progress_item_id, global_ec3.project_construction_quantity_contract_bill_part.progress_item_name, global_ec3.project_construction_quantity_contract_bill_part.project_bill_quantity_detail_id, global_ec3.project_construction_quantity_contract_bill_part.project_part_id, global_ec3.project_construction_quantity_contract_bill_part.project_part_name, global_ec3.project_construction_quantity_contract_bill_part.project_unit_work_id, global_ec3.project_construction_quantity_contract_bill_part.project_unit_work_name, global_ec3.project_construction_quantity_contract_bill_part.quantity, global_ec3.project_construction_quantity_contract_bill_part.total_bill_quantity, global_ec3.project_construction_quantity_contract_bill_part.total_quantity, global_ec3.project_construction_quantity_contract_bill_part.unit, global_ec3.project_construction_quantity_contract_bill_part.unit_work_id, global_ec3.project_construction_quantity_contract_bill_part.value_type, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.id)->global_ec3.project_construction_quantity_contract_bill_part.id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.org_id)->global_ec3.project_construction_quantity_contract_bill_part.org_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.construction_quantity_id)->global_ec3.project_construction_quantity_contract_bill_part.construction_quantity_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.project_bill_quantity_detail_id)->global_ec3.project_construction_quantity_contract_bill_part.project_bill_quantity_detail_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.unit_work_id)->global_ec3.project_construction_quantity_contract_bill_part.unit_work_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.project_unit_work_id)->global_ec3.project_construction_quantity_contract_bill_part.project_unit_work_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.project_unit_work_name)->global_ec3.project_construction_quantity_contract_bill_part.project_unit_work_name, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.entry_work_id)->global_ec3.project_construction_quantity_contract_bill_part.entry_work_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.entry_work_name)->global_ec3.project_construction_quantity_contract_bill_part.entry_work_name, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.value_type)->global_ec3.project_construction_quantity_contract_bill_part.value_type, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.progress_item_id)->global_ec3.project_construction_quantity_contract_bill_part.progress_item_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.progress_item_name)->global_ec3.project_construction_quantity_contract_bill_part.progress_item_name, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.project_part_id)->global_ec3.project_construction_quantity_contract_bill_part.project_part_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.project_part_name)->global_ec3.project_construction_quantity_contract_bill_part.project_part_name, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.unit)->global_ec3.project_construction_quantity_contract_bill_part.unit, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.quantity)->global_ec3.project_construction_quantity_contract_bill_part.quantity, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.total_quantity)->global_ec3.project_construction_quantity_contract_bill_part.total_quantity, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.order_no)->global_ec3.project_construction_quantity_contract_bill_part.order_no, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.parent_id)->global_ec3.project_construction_quantity_contract_bill_part.parent_id, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.level)->global_ec3.project_construction_quantity_contract_bill_part.level, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.is_leaf)->global_ec3.project_construction_quantity_contract_bill_part.is_leaf, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.current_bill_quantity)->global_ec3.project_construction_quantity_contract_bill_part.current_bill_quantity, funcs:firstrow(global_ec3.project_construction_quantity_contract_bill_part.total_bill_quantity)->global_ec3.project_construction_quantity_contract_bill_part.total_bill_quantity | 27.5 MB | N/A      |
// |   └─HashJoin_25                  | 92.34    | 2311373.33  | 33612   | root      |                                                                                           | time:1m47.1s, loops:39, build_hash_table:{total:152.9ms, fetch:152ms, build:927.6µs}, probe:{concurrency:16, total:20m56.1s, max:1m47.1s, probe:20m51.6s, fetch:4.57s}                                                                                                                                                                                                                                                                                                                                                                                                                   | CARTESIAN inner join, other cond:gt(locate(cast(global_ec3.project_construction_quantity_contract_bill_part.id, var_string(100)), global_ec3.project_construction_quantity_contract_bill_part.full_id), 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | 1.88 MB | 0 Bytes  |
// |     ├─IndexLookUp_35(Build)      | 1.02     | 1155080.67  | 8403    | root      |                                                                                           | time:151.4ms, loops:10, index_task: {total_time: 37.4ms, fetch_handle: 37.3ms, build: 61.7µs, wait: 15.1µs}, table_task: {total_time: 327.1ms, num: 7, concurrency: 16}, next: {wait_index: 55.3ms, wait_table_lookup_build: 0s, wait_table_lookup_resp: 67.6ms}                                                                                                                                                                                                                                                                                                                         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 2.32 MB | N/A      |
// |     │ ├─IndexRangeScan_32(Build) | 39705.48 | 3177431.29  | 56588   | cop[tikv] | table:c_part, index:PRIMARY(tenant, org_id, id)                                           | time:30.8ms, loops:58, cop_task: {num: 1, max: 30.3ms, proc_keys: 0, tot_proc: 1.53µs, tot_wait: 60.7µs, rpc_num: 1, rpc_time: 30.3ms, copr_cache_hit_ratio: 1.00, build_task_duration: 30.3µs, max_distsql_concurrency: 1}, tikv_task:{time:32ms, loops:60}, scan_detail: {get_snapshot_time: 23.5µs, rocksdb: {block: {}}}                                                                                                                                                                                                                                                             | range:["cscrc" 1655078378418688,"cscrc" 1655078378418688], keep order:false                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | N/A     | N/A      |
// |     │ └─Selection_34(Probe)      | 1.02     | 23271780.76 | 8403    | cop[tikv] |                                                                                           | time:296ms, loops:19, cop_task: {num: 7, max: 33ms, min: 20.4ms, avg: 22.6ms, p95: 33ms, max_proc_keys: 20480, p95_proc_keys: 20480, tot_proc: 31.5ms, tot_wait: 286.9µs, rpc_num: 7, rpc_time: 158ms, copr_cache_hit_ratio: 0.57, build_task_duration: 445.6µs, max_distsql_concurrency: 1}, tikv_task:{proc max:40ms, min:0s, avg: 16ms, p80:28ms, p95:40ms, iters:90, tasks:7}, scan_detail: {total_process_keys: 24544, total_process_keys_size: 11039346, total_keys: 24550, get_snapshot_time: 98.5µs, rocksdb: {key_skipped_count: 48927, block: {cache_hit_count: 485}}}         | eq(global_ec3.project_construction_quantity_contract_bill_part.construction_quantity_id, 1784483382014976), eq(global_ec3.project_construction_quantity_contract_bill_part.is_leaf, 1), eq(global_ec3.project_construction_quantity_contract_bill_part.is_removed, 0), eq(global_ec3.project_construction_quantity_contract_bill_part.project_bill_quantity_detail_id, 1670632461654168), ne(global_ec3.project_construction_quantity_contract_bill_part.quantity, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | N/A     | N/A      |
// |     │   └─TableRowIDScan_33      | 39705.48 | 23152664.31 | 56588   | cop[tikv] | table:c_part                                                                              | tikv_task:{proc max:40ms, min:0s, avg: 16ms, p80:28ms, p95:40ms, iters:90, tasks:7}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | keep order:false                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | N/A     | N/A      |
// |     └─IndexLookUp_45(Probe)      | 90.86    | 1156221.29  | 24252   | root      |                                                                                           | time:242ms, loops:25, index_task: {total_time: 37.3ms, fetch_handle: 37.3ms, build: 8.37µs, wait: 12.9µs}, table_task: {total_time: 242ms, num: 7, concurrency: 16}, next: {wait_index: 55.3ms, wait_table_lookup_build: 0s, wait_table_lookup_resp: 105.4ms}                                                                                                                                                                                                                                                                                                                            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | 10.3 MB | N/A      |
// |       ├─IndexRangeScan_42(Build) | 39705.48 | 3177431.29  | 56588   | cop[tikv] | table:project_construction_quantity_contract_bill_part, index:PRIMARY(tenant, org_id, id) | time:33.8ms, loops:58, cop_task: {num: 1, max: 33.5ms, proc_keys: 0, tot_proc: 1.1µs, tot_wait: 39µs, rpc_num: 1, rpc_time: 33.5ms, copr_cache_hit_ratio: 1.00, build_task_duration: 10.1µs, max_distsql_concurrency: 1}, tikv_task:{time:32ms, loops:60}, scan_detail: {get_snapshot_time: 15.4µs, rocksdb: {block: {}}}                                                                                                                                                                                                                                                                | range:["cscrc" 1655078378418688,"cscrc" 1655078378418688], keep order:false                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | N/A     | N/A      |
// |       └─Selection_44(Probe)      | 90.86    | 23271780.76 | 24252   | cop[tikv] |                                                                                           | time:199ms, loops:33, cop_task: {num: 7, max: 90.8ms, min: 9.05ms, avg: 22.7ms, p95: 90.8ms, max_proc_keys: 20480, p95_proc_keys: 20480, tot_proc: 57.7ms, tot_wait: 237.5µs, rpc_num: 7, rpc_time: 158.8ms, copr_cache_hit_ratio: 0.86, build_task_duration: 222.2µs, max_distsql_concurrency: 1}, tikv_task:{proc max:60ms, min:8ms, avg: 24.6ms, p80:40ms, p95:60ms, iters:90, tasks:7}, scan_detail: {total_process_keys: 20480, total_process_keys_size: 9196862, total_keys: 20484, get_snapshot_time: 79.1µs, rocksdb: {key_skipped_count: 40805, block: {cache_hit_count: 385}}} | eq(global_ec3.project_construction_quantity_contract_bill_part.construction_quantity_id, 1784483382014976), eq(global_ec3.project_construction_quantity_contract_bill_part.is_removed, 0), eq(global_ec3.project_construction_quantity_contract_bill_part.value_type, "part")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | N/A     | N/A      |
// |         └─TableRowIDScan_43      | 39705.48 | 23152664.31 | 56588   | cop[tikv] | table:project_construction_quantity_contract_bill_part                                    | tikv_task:{proc max:56ms, min:8ms, avg: 24ms, p80:40ms, p95:56ms, iters:90, tasks:7}

func TestSelectStmtFullSQLLog(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(151300), "disk": int64(9527), "rows": int64(1024), "maxAct": int64(2024),
		"rru": int(1111), "wru": int(22),
		"err": "mock sql error",
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()
	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	sessVars := tk.Session().GetSessionVars()

	sql := createSelectTestSQL()
	tk.MustExec(sql)
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)

	times := logData["times"].(map[string]any)
	require.NotContains(t, times, "tx")
	require.NotContains(t, logData, "tx")

	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "tenant": "cscrc", "across": "global_sq|global_qa",
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "ec-analysis-service", "product": "",
		},
		"cat": "dml", "tp": "select", "inTX": false, "maxAct": float64(2024),
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
		},
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"ru":     map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem":    float64(151300), "disk": float64(9527), "rows": float64(1024),
		"digest": "422a8fb24253641cc985c5125d28b382eb4fe90c7ca01050e1e5dd0b39b2c673",
		"sql":    sql,
		"error":  "mock sql error",
	}, logData)
}

func TestSelectStmtFullSQLLogInTX(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(151300), "disk": int64(9527), "rows": int64(1024), "maxAct": int64(2024),
		"rru": int(1111), "wru": int(22),
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()
	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	sessVars := tk.Session().GetSessionVars()

	sql := createSelectTestSQL()
	tk.MustExec("begin")
	defer tk.MustExec("commit")
	tk.MustExec(sql)
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)

	times := logData["times"].(map[string]any)
	require.NotContains(t, times, "tx")
	require.NotContains(t, logData, "tx")

	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "tenant": "cscrc", "across": "global_sq|global_qa",
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "ec-analysis-service", "product": "",
		},
		"cat": "dml", "tp": "select", "inTX": true, "maxAct": float64(2024),
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"ru":     map[string]any{"rru": float64(1111), "wru": float64(22)},
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
		},
		"mem": float64(151300), "disk": float64(9527), "rows": float64(1024),
		"digest": "422a8fb24253641cc985c5125d28b382eb4fe90c7ca01050e1e5dd0b39b2c673",
		"sql":    sql,
	}, logData)
}

func TestUpdateStmtFullSQLLog(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(45832), "disk": int64(9527),
		"rru": int(1111), "wru": int(22),
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()
	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	sessVars := tk.Session().GetSessionVars()

	sql := createUpdateTestSQL()
	tk.MustExec(sql)
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)

	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "tenant": "gslq",
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "qa-cloud-service", "product": "pf", "pkg": "@mctech/dp-impala",
		},
		"cat": "dml", "tp": "update", "inTX": false, "maxAct": float64(1),
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
			"tx":  map[string]any{"prewrite": "1.032s", "commit": "100ms"},
		},
		"tx":  map[string]any{"affected": float64(1), "keys": float64(1), "size": float64(44)},
		"ru":  map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem": float64(45832), "disk": float64(9527),
		"rows":   float64(0),
		"digest": "5e4bdd67e44582ea529c3e9b28973aa072ad15377afcf552dea969e320ae5940",
		"sql":    sql,
	}, logData)
}

func TestUpdateStmtFullSQLLogInTx(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(45832), "disk": int64(9527),
		"rru": int(1111), "wru": int(22),
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()
	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	sessVars := tk.Session().GetSessionVars()

	sql := createUpdateTestSQL()
	tk.MustExec("begin")
	defer tk.MustExec("commit")
	tk.MustExec(sql)
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)

	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "global_ec3", "usr": "root", "tenant": "gslq",
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
			"app":     "qa-cloud-service", "product": "pf", "pkg": "@mctech/dp-impala",
		},
		"cat": "dml", "tp": "update", "inTX": true, "maxAct": float64(1),
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
		},
		"tx":  map[string]any{"affected": float64(1), "keys": float64(0), "size": float64(0)},
		"ru":  map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem": float64(45832), "disk": float64(9527),
		"rows":   float64(0),
		"digest": "5e4bdd67e44582ea529c3e9b28973aa072ad15377afcf552dea969e320ae5940",
		"sql":    sql,
	}, logData)
}

func TestCommitStmtFullSQLLogInTx(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true}),
	)
	failpoint.Enable("github.com/pingcap/tidb/executor/CreateeWarnings",
		mock.M(t, "true"),
	)
	now := time.Now().Format("2006-01-02 15:04:05.000")
	failpoint.Enable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData", mock.M(t, map[string]any{
		"maxCop":    map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": int(8)},
		"startedAt": now, "mem": int64(15300), "disk": int64(25300),
		"rru": int(1111), "wru": int(22),
	}))
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/executor/CreateeWarnings")
		failpoint.Disable("github.com/pingcap/tidb/mctech/interceptor/MockTraceLogData")
	}()
	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	sessVars := tk.Session().GetSessionVars()

	sql := createUpdateTestSQL()
	tk.MustExec("begin")
	tk.MustExec(sql)
	tk.MustExec("commit")
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)

	require.Equal(t, map[string]any{
		"db": "global_ec3", "dbs": "", "usr": "root",
		"at": now, "txId": interceptor.EncodeForTest(sessVars.TxnCtx.StartTS),
		"client": map[string]any{
			"conn":    interceptor.EncodeForTest(sessVars.ConnectionID),
			"address": interceptor.FormatAddressTest(sessVars.ConnectionInfo),
		},
		"cat": "tx", "tp": "commit", "inTX": false, "maxAct": float64(0),
		"maxCop": map[string]any{"procAddr": "tikv01:21060", "procTime": "128ms", "tasks": float64(8)},
		"times": map[string]any{
			"all": "3.315821ms", "tidb": "11.201s", "parse": "176.943µs", "plan": "1.417613ms", "ready": "2.315821ms", "send": "1ms",
			"cop": map[string]any{"wall": "128ms", "tikv": "98ms", "tiflash": "12µs"},
			"tx":  map[string]any{"prewrite": "1.032s", "commit": "100ms"},
		},
		"tx":  map[string]any{"affected": float64(0), "keys": float64(2), "size": float64(122)},
		"ru":  map[string]any{"rru": float64(1111), "wru": float64(22)},
		"mem": float64(15300), "disk": float64(25300),
		"rows":     float64(0),
		"digest":   "9505cacb7c710ed17125fcc6cb3669e8ddca6c8cd8af6a31f6b3cd64604c3098",
		"warnings": map[string]any{"topN": []any{map[string]any{"msg": "this is for test warning", "extra": false}}, "total": float64(1)},
		"sql":      "commit",
	}, logData)
}

func TestFullSQLLogDbCheckerError(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]bool{
			"Metrics.SqlTrace.Enabled": true, "Tenant.Enabled": true,
			"DbChecker.Enabled": true,
		}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/mctech/preps/DbCheckError")
	}()
	store := testkit.CreateMockStore(t)
	tk := initDbAndData(t, store)
	failpoint.Enable("github.com/pingcap/tidb/mctech/preps/DbCheckError", mock.M(t, "true"))
	sql := "/*& impersonate: tenant_only */ " + createUpdateTestSQL()
	_, err := tk.Exec(sql)
	require.Error(t, err, "dbs not allow in the same statement")
	logData, err := interceptor.GetFullQueryTraceLog(tk.Session())
	require.NoError(t, err)
	require.NotNil(t, logData)
	rawError := logData["error"]
	require.Contains(t, rawError, "dbs not allow in the same statement")
}

type sqlCompressCase struct {
	threshold         int
	sqlLen            int
	must              bool
	expectPrefixEnd   int
	expectSuffixStart int
}

func (c sqlCompressCase) String() string {
	return fmt.Sprintf("threshold: %d, sqlLen: %d", c.threshold, c.sqlLen)
}

func TestSqlCompress(t *testing.T) {
	cases := []sqlCompressCase{
		// 当 threshold >= sqlReserveBothLen
		{250, 300, true, 150, 200},
		{200, 250, true, 100, 150},

		{250, 250, false, -1, -1},
		{250, 200, false, -1, -1},
		{250, 150, false, -1, -1},
		{250, 100, false, -1, -1},
		{250, 50, false, -1, -1},
		{200, 200, false, -1, -1},
		// 当 sqlReserveBothLen > threshold >= sqlPrefixLen
		{150, 250, true, 100, 200},
		{150, 200, true, 100, 150},
		{150, 170, true, 100, 120},
		{101, 150, true, 100, 149},
		{100, 150, true, 100, 150},

		{160, 150, false, -1, -1},
		{150, 150, false, -1, -1},
		{100, 90, false, -1, -1},
		{90, 80, false, -1, -1},
		// 当 sqlPrefixLen > threshold
		{90, 120, true, 90, 120},
		{90, 100, true, 90, 100},
		{70, 170, true, 70, 170},

		{90, 90, false, -1, -1},
		{70, 60, false, -1, -1},
	}

	for i, c := range cases {
		prefixEnd, suffixStart, ok := interceptor.MustCompressForTest(c.sqlLen, c.threshold)
		require.Equal(t, c.must, ok, fmt.Sprintf("case %d, %v", i, c))
		if ok {
			require.Equal(t, c.expectPrefixEnd, prefixEnd, fmt.Sprintf("case %d, %v", i, c))
			require.Equal(t, c.expectSuffixStart, suffixStart, fmt.Sprintf("case %d, %v", i, c))
		}
	}
}

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_ec3")
	tk.MustExec("create database global_ec3")
	tk.MustExec("use global_ec3")
	s := tk.Session()
	s.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}
	return tk
}

func initDbAndData(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := initMock(t, store)
	createSQL := strings.Join([]string{
		"CREATE TABLE `project_construction_quantity_contract_bill_part` (",
		"	`tenant` varchar(50) NOT NULL COMMENT '租户编码',",
		"	`org_id` bigint(20) NOT NULL COMMENT '组织id',",
		"	`id` bigint(20) NOT NULL COMMENT 'id',",
		"	`construction_quantity_id` bigint(20) NULL COMMENT '施工完成id',",
		"	`construction_quantity_contract_bill_id` bigint(20) NULL COMMENT '施工完成-原合同清单id',",
		"	`project_bill_quantity_detail_id` bigint(20) DEFAULT NULL COMMENT '原清单id',",
		"	`unit_work_id` bigint(20) DEFAULT NULL COMMENT 'wbs字典id',",
		"	`project_unit_work_id` bigint(20) DEFAULT NULL COMMENT '单位工程id',",
		"	`project_unit_work_name` varchar(255) DEFAULT NULL COMMENT '单位工程名称',",
		"	`progress_item_id` bigint(20) DEFAULT NULL COMMENT '形象进度id',",
		"	`progress_item_name` varchar(255) DEFAULT NULL COMMENT '形象进度名称',",
		"	`project_part_id` bigint(20) DEFAULT NULL COMMENT '部位id',",
		"	`project_part_name` varchar(255) DEFAULT NULL COMMENT '部位名称',",
		"	`unit` varchar(50) DEFAULT NULL COMMENT '单位',",
		"	`quantity` decimal(28,5) DEFAULT NULL COMMENT '完成形象量',",
		"	`total_quantity` decimal(28,5) DEFAULT NULL COMMENT '开累-形象量',",
		"	`current_bill_quantity` decimal(28,5) DEFAULT NULL COMMENT '本期数量',",
		"	`total_bill_quantity` decimal(28,5) DEFAULT NULL COMMENT '开累数量',",
		"	`order_no` bigint(20) NULL COMMENT '排序',",
		"	`parent_id` bigint(20) NULL COMMENT '父id',",
		"	`level` bigint(20) NULL COMMENT '级别',",
		"	`full_id` varchar(255) NULL COMMENT 'fullId',",
		"	`is_leaf` tinyint(1) DEFAULT NULL COMMENT '是否末级',",
		"	`is_removed` tinyint(1) NULL DEFAULT '0' COMMENT '删除标记',",
		"	`creator` bigint(20) NULL COMMENT '记录创建人',",
		"	`created_at` datetime NULL COMMENT '记录创建时间',",
		"	`reviser` bigint(20) NULL COMMENT '记录修改人',",
		"	`updated_at` datetime NULL COMMENT '记录修改时间',",
		"	`version` bigint(20) NULL COMMENT '记录版本号',",
		"	`value_type` varchar(50) DEFAULT NULL COMMENT '开项类型：entryWork || part',",
		"	`entry_work_id` bigint(20) DEFAULT NULL COMMENT '分部分项id',",
		"	`entry_work_name` varchar(255) DEFAULT NULL COMMENT '分部分项name',",
		"	PRIMARY KEY (`tenant`,`org_id`,`id`) /*T![clustered_index] NONCLUSTERED */",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='施工完成量-原合同清单-形象进度/部位'",
	}, "\n")
	tk.MustExec(createSQL)
	tk.MustExec("/*& tenant:'gslq' */ insert into project_construction_quantity_contract_bill_part (tenant, org_id, id) values ('cscrc', 1, 1), ('gslq', 2, 2)")
	return tk
}

func createUpdateTestSQL() string {
	return strings.Join([]string{
		"/* from:'qa-cloud-service.pf', addr:'10.180.108.236' */",
		"/* package:'@mctech/dp-impala' */",
		"/*& tenant:'gslq' */",
		"update project_construction_quantity_contract_bill_part",
		"set is_removed = true",
	}, "\n")
}

func createSelectTestSQL() string {
	return strings.Join([]string{
		"/* from:'ec-analysis-service', addr:'10.180.108.236' */",
		"/*& tenant:'cscrc' */",
		"/*& across:global_sq,global_qa */",
		"WITH p_part AS (",
		"  SELECT",
		"    id,",
		"    org_id,",
		"    construction_quantity_id,",
		"    project_bill_quantity_detail_id,",
		"    unit_work_id,",
		"    project_unit_work_id,",
		"    project_unit_work_name,",
		"    entry_work_id,",
		"    entry_work_name,",
		"    value_type,",
		"    progress_item_id,",
		"    progress_item_name,",
		"    project_part_id,",
		"    project_part_name,",
		"    unit,",
		"    quantity,",
		"    total_quantity,",
		"    order_no,",
		"    parent_id,",
		"    LEVEL,",
		"    is_leaf,",
		"    current_bill_quantity,",
		"    total_bill_quantity",
		"  FROM",
		"    project_construction_quantity_contract_bill_part",
		"  WHERE",
		"    construction_quantity_id = 1784483382014976",
		"    AND org_id = 1655078378418688",
		"    AND is_removed = FALSE",
		")",
		"SELECT",
		"  DISTINCT p_part.*",
		"FROM",
		"  project_construction_quantity_contract_bill_part c_part",
		"  INNER JOIN p_part ON LOCATE(CAST(p_part.id AS CHAR(100)), c_part.full_id) > 0",
		"WHERE",
		"  c_part.construction_quantity_id = 1784483382014976",
		"  AND c_part.org_id = 1655078378418688",
		"  AND c_part.is_removed = FALSE",
		"  AND c_part.is_leaf = TRUE",
		"  AND IF(1670632461654168 > 0, 1 = 1, p_part.is_leaf = TRUE)",
		"  AND IF(",
		"    1670632461654168 > 0,",
		"    p_part.value_type = 'part',",
		"    1 = 1",
		"  )",
		"  AND IF(1670632461654168 > 0, c_part.quantity <> 0, 1 = 1)",
		"  AND IF(",
		"    1670632461654168 > 0,",
		"    c_part.project_bill_quantity_detail_id = 1670632461654168,",
		"    1 = 1",
		"  )",
		"ORDER BY",
		"  p_part.level DESC,",
		"  p_part.order_no",
	}, "\n")
}
