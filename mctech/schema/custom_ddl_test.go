package schema

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/auth"
	_ "github.com/pingcap/tidb/parser/test_driver"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func initMock(t *testing.T, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists global_platform")
	tk.MustExec("create database global_platform")
	tk.MustExec("use global_platform")
	s := tk.Session()
	s.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}
	return tk
}

var createTableSQL = strings.Join([]string{
	"create table version_table (",
	"a varchar(10),",
	"b int,",
	"c timestamp(6) not null default current_timestamp(6) on update current_timestamp(6),",
	"primary key(a)",
	")",
}, "\n")

func TestMCTechSequenceDefaultValueSchemaTest(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)

	session := tk.Session()
	ctx, _ := mctech.WithNewContext(session)
	tk.MustExecWithContext(ctx, createTableSQL)
	res := tk.MustQuery("show create table version_table")
	createSQL := res.Rows()[0][1].(string)
	expected := strings.Join([]string{
		"CREATE TABLE `version_table` (",
		"  `a` varchar(10) NOT NULL,",
		"  `b` int(11) DEFAULT NULL,",
		"  `c` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),",
		"  `__version` bigint(20) NOT NULL DEFAULT MCTECH_SEQUENCE ON UPDATE MCTECH_SEQUENCE,",
		"  PRIMARY KEY (`a`) /*T![clustered_index] NONCLUSTERED */",
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
			"[c timestamp(6) NO  CURRENT_TIMESTAMP(6) DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)]",
			"[__version bigint(20) NO  MCTECH_SEQUENCE DEFAULT_GENERATED on update MCTECH_SEQUENCE]",
		}, "\n"),
	)
}

func TestMCTechSequenceDefaultValueAlterSchemaTest(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)

	tk.MustExec(strings.Join([]string{
		"create table version_table (",
		"a varchar(10)",
		",t timestamp(6) not null default current_timestamp(6) on update current_timestamp(6)",
		",v bigint(20) not null default mctech_sequence on update mctech_sequence",
		")",
	}, "\n"))
	tk.MustExec("alter table version_table add column `stamp` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)")
	tk.MustExec("alter table version_table add column `__version` bigint(20) NOT NULL DEFAULT MCTECH_SEQUENCE ON UPDATE MCTECH_SEQUENCE")

	res := tk.MustQuery("show create table version_table")
	createSQL := res.Rows()[0][1].(string)
	expected := strings.Join([]string{
		"CREATE TABLE `version_table` (",
		"  `a` varchar(10) DEFAULT NULL,",
		"  `t` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),",
		"  `v` bigint(20) NOT NULL DEFAULT MCTECH_SEQUENCE ON UPDATE MCTECH_SEQUENCE,",
		"  `stamp` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),",
		"  `__version` bigint(20) NOT NULL DEFAULT MCTECH_SEQUENCE ON UPDATE MCTECH_SEQUENCE",
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
			"[a varchar(10) YES  <nil> ]",
			"[t timestamp(6) NO  CURRENT_TIMESTAMP(6) DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)]",
			"[v bigint(20) NO  MCTECH_SEQUENCE DEFAULT_GENERATED on update MCTECH_SEQUENCE]",
			"[stamp timestamp(6) NO  CURRENT_TIMESTAMP(6) DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)]",
			"[__version bigint(20) NO  MCTECH_SEQUENCE DEFAULT_GENERATED on update MCTECH_SEQUENCE]",
		}, "\n"),
	)
}

func TestMCTechSequenceDefaultValueOnInsertTest(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)

	session := tk.Session()
	ctx, _ := mctech.WithNewContext(session)
	tk.MustExecWithContext(ctx, createTableSQL)
	tk.MustExec(
		`insert into version_table
		(a, b)
		values ('a', ifnull(sleep(0.01), 1)), ('b', ifnull(sleep(0.01),2)), ('c', ifnull(sleep(0.01),3)), ('d', ifnull(sleep(0.01),4))
		`)
	res := tk.MustQuery("select a, b, c, __version from version_table")
	seqs := map[string]any{}
	stamps := map[string]any{}
	rows := res.Rows()
	for _, row := range rows {
		stamps[row[2].(string)] = true
		seqs[row[3].(string)] = true
	}
	require.Len(t, seqs, len(rows))
	require.Len(t, stamps, 1)

	tk.MustExec("update version_table set b = -1")
	res = tk.MustQuery("select * from version_table")
	rows = res.Rows()
	time.Sleep(time.Second)
	for _, row := range rows {
		seqs[row[3].(string)] = true
		stamps[row[2].(string)] = true
	}
	require.Len(t, seqs, len(rows)*2)
	require.Len(t, stamps, 2)
	// fmt.Printf("%v", rows)
}

func TestMCTechSequenceDefaultValueInitTest(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)

	tk.MustExec(strings.Join([]string{
		"create table version_table (",
		"a varchar(10)",
		",b int",
		")",
	}, "\n"))

	tk.MustExec(
		`insert into version_table
		(a, b)
		values ('a', 1), ('b', 2), ('c', 3), ('d', 4)
		`)

	tk.MustExec("alter table version_table add column `stamp` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)")
	tk.MustExec("alter table version_table add column `__version` bigint(20) NOT NULL DEFAULT MCTECH_SEQUENCE ON UPDATE MCTECH_SEQUENCE")

	res := tk.MustQuery("select stamp, __version from version_table where __version is not null")
	rows := res.Rows()
	require.Len(t, rows, 4)
	fmt.Printf("%v", rows)
}

func TestBigintDefaultValueOnInsertTest(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)

	tk.MustExec("create table t1 (id int primary key, c1 bigint not null default 3)")
	tk.MustExec(`insert into t1 (id) values (1)`)
	res := tk.MustQuery("select * from t1")
	rows := res.Rows()
	require.Equal(t, "3", rows[0][1])
}

func TestInsertSelectUseSequenceTest(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := initMock(t, store)

	tk.MustExec("create table t1 (id int primary key, c1 bigint not null)")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec(`insert into t2 (id) values (1),(2)`)
	tk.MustExec(`insert into t1 (id, c1) select id, mc_seq() from t2`)
	res := tk.MustQuery("select id, c1 from t1")
	rows := res.Rows()
	seqs := map[string]any{}
	for _, row := range rows {
		seqs[row[1].(string)] = true
	}
	require.Len(t, seqs, len(rows))
}
