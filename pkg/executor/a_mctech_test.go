// add by zhangbing

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

type mctechStmtCases struct {
	source string
	expect string
}

// func TestMain(t *testing.M) {

// }

func TestMCTechStatementsSummary(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []*mctechStmtCases{
		{"mctech select * from information_schema.statements_summary", "0  |none|test|18446744073709551615|{}|SELECT * FROM `information_schema`.`statements_summary`"},
	}

	for _, c := range cases {
		tk.MustQuery(c.source).Check(
			testkit.RowsWithSep("|", c.expect))
	}
}
