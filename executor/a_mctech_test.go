package executor_test

// import (
// 	"testing"

// 	"github.com/pingcap/tidb/testkit"
// )

// type mctechStmtCases struct {
// 	source string
// 	expect string
// }

// // func TestMain(t *testing.M) {

// // }

// func TestMCTechStatementsSummary(t *testing.T) {
// 	store, clean := testkit.CreateMockStore(t)
// 	defer clean()
// 	tk := testkit.NewTestKit(t, store)
// 	tk.MustExec("use test")

// 	cases := []*mctechStmtCases{
// 		{"mctech select * from information_schema.statements_summary", "0  |none|18446744073709551615|{}|SELECT * FROM `information_schema`.`statements_summary`"},
// 	}

// 	for _, c := range cases {
// 		tk.MustQuery(c.source).Check(
// 			testkit.RowsWithSep("|", c.expect))
// 	}
// }
