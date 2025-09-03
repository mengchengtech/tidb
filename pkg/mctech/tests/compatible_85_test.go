package test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssueCantFindColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// tk.MustExec("set global tidb_enable_inl_join_inner_multi_pattern = 'ON'")
	// tk.MustExec("set global tidb_remove_orderby_in_subquery = 'OFF'")
	// tk.MustExec("set global tidb_opt_projection_push_down = 'OFF'")
	// tk.MustExec("set global tidb_opt_prefer_range_scan = 'OFF'")

	tk.MustExec("drop table if exists t1")
	createSQL := `create table t1 (id bigint, is_removed boolean, year int, status varchar(50), primary key (id))`
	tk.MustExec(createSQL)

	selectSQL := `with main as (select * from t1 where is_removed = false), 
min_not_submitted as (select id from main where status = 'unsubmitted' order by year limit 1),
max_submitted as (select max(id) id from main)
select 
  main.id = min_not_submitted.id AS is_submit,
  if(main.id = max_submitted.id and main.status='submitted', true, false) as is_revoke
from main left join min_not_submitted on 1=1 left join max_submitted on 1=1
`
	tk.MustExec(selectSQL)
}
