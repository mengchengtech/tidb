package schema

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/prapared"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

var createTableSQL = strings.Join([]string{
	"create table version_table (",
	"a varchar(10),",
	"b int,",
	"c timestamp not null default current_timestamp on update current_timestamp,",
	"primary key(a)",
	")",
}, "\n")

func TestMCTechSequenceDefaultValueSchemaTest(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := initMock(t, store)

	session := tk.Session()
	mctech.SetHandlerFactory(session, prapared.GetHandlerFactory())
	tk.MustExec(createTableSQL)
	res := tk.MustQuery("show create table version_table")
	createSQL := res.Rows()[0][1].(string)
	expected := strings.Join([]string{
		"CREATE TABLE `version_table` (",
		"  `a` varchar(10) NOT NULL,",
		"  `b` int(11) DEFAULT NULL,",
		"  `c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,",
		"  `__version` bigint(20) NOT NULL DEFAULT MCTECH_SEQUENCE ON UPDATE MCTECH_SEQUENCE,",
		"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"}, "\n")
	require.Equal(t, expected, createSQL)
	res = tk.MustQuery("show columns from version_table")
	lst := []string{}
	for _, row := range res.Rows() {
		lst = append(lst, fmt.Sprintf("%v", row))
	}

	require.Equal(t,
		strings.Join(lst, "\n"),
		strings.Join([]string{
			"[a varchar(10) NO PRI <nil> ]",
			"[b int(11) YES  <nil> ]",
			"[c timestamp NO  CURRENT_TIMESTAMP DEFAULT_GENERATED on update CURRENT_TIMESTAMP]",
			"[__version bigint(20) NO  MCTECH_SEQUENCE DEFAULT_GENERATED on update MCTECH_SEQUENCE]",
		}, "\n"),
	)
}

func TestMCTechSequenceDefaultValueOnInsertTest(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := initMock(t, store)

	session := tk.Session()
	mctech.SetHandlerFactory(session, prapared.GetHandlerFactory())
	tk.MustExec(createTableSQL)
	tk.MustExec(
		`insert into version_table
		(a, b)
		values ('a', 1), ('b', 2), ('c', 3), ('d', 4)
		`)
	res := tk.MustQuery("select * from version_table")
	mp := map[string]any{}
	rows := res.Rows()
	for _, row := range rows {
		mp[row[3].(string)] = true
	}
	require.Len(t, mp, len(rows))

	tk.MustExec("update version_table set b = -1")
	res = tk.MustQuery("select * from version_table")
	rows = res.Rows()
	for _, row := range rows {
		mp[row[3].(string)] = true
	}
	require.Len(t, mp, len(rows)*2)
	// fmt.Printf("%v", rows)
}