// add by zhangbing

package core_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type testBuildMCTechCase struct {
	from        string
	pkg         string
	includes    []string
	excludes    []string
	tenantOnly  bool
	tenantOmit  bool
	mpp         string
	tenantRole  string
	impersonate bool
	tenant      string
	expected    string
}

func (c *testBuildMCTechCase) Source() string {
	return fmt.Sprintf("%v", c)
}

func initSession(tk *testkit.TestKit, user string, roles ...string) {
	session := tk.Session()
	vars := session.GetSessionVars()
	vars.User = &auth.UserIdentity{Username: user, Hostname: "%"}

	ar := make([]*auth.RoleIdentity, 0, len(roles))
	if len(roles) > 0 {
		for _, r := range roles {
			if r != "" {
				ar = append(ar, &auth.RoleIdentity{Username: r, Hostname: "%"})
			}
		}
	}
	vars.ActiveRoles = ar
}

func TestBuildMCTechCommon(t *testing.T) {
	fullPath, err := filepath.Abs("../../mctech/udf/data")
	require.NoError(t, err)
	// unixMilli = 1697003594437
	datetime := "2023-10-11 13:53:14.437"
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]any{
			"Tenant.Enabled":              true,
			"DbChecker.Enabled":           true,
			"SQLChecker.Enabled":          true,
			"Metrics.SqlTrace.FullSqlDir": fullPath,
		}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mock.M(t, map[string]any{"DWIndex.Current": map[string]any{"current": 1}}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade", mock.M(t, "true"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")

	now := time.Now()
	cases := []struct {
		sql   string
		check func(rs sqlexec.RecordSet)
		mock  func(*testkit.TestKit)
	}{
		{
			sql: "mctech show help",
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 1)
				row := fetchResultsetFirstRow(t, rs)
				value := row["Help_content"]
				require.Contains(t, value, "FUNCTION::")
				require.NotContains(t, value, "mctech_version_just_pass")
			},
		},
		{
			sql: "mctech show help true",
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 1)
				row := fetchResultsetFirstRow(t, rs)
				value := row["Help_content"]
				require.Contains(t, value, "FUNCTION::")
				require.Contains(t, value, "mctech_version_just_pass")
			},
		},
		{
			sql: "mctech show database constraints",
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 16)
				rows := fetchResultsetRows(t, rs)
				require.Len(t, rows, 4)
				for _, row := range rows {
					loadedAt, err := time.Parse("2006-01-02 15:04:05.000000", row["LOADED_AT"].(string))
					require.NoError(t, err)
					require.True(t, loadedAt.After(now))
					delete(row, "LOADED_AT")
				}
				expect := []map[string]any{
					{
						"RULE_ID": uint64(1), "INVOKER_NAME": "*", "INVOKER_TYPE": "both", "ALLOW_ALL_DBS": uint64(0), "CROSS_DBS": "global_mtlp,global_ma", "ENABLED": uint64(1),
						"REMARK": "同一条sql语句中允许同时使用给定的数据库", "LOADED_STATE": "success", "LOADED_MESSAGE": "Loaded Success", "LOADED_DETAIL_ALLOW_ALL_DBS": uint64(0),
						"LOADED_DETAIL_SERVICE": "*", "LOADED_DETAIL_PACKAGE": "*", "LOADED_DETAIL_CROSS_DBS": "[global_mtlp global_ma]", "LOADED_DETAIL_FILTER_GLOBAL": nil,
						"LOADED_DETAIL_FILTER_PATTERNS": nil,
					},
					{
						"RULE_ID": uint64(2), "INVOKER_NAME": "*", "INVOKER_TYPE": "both", "ALLOW_ALL_DBS": uint64(0), "CROSS_DBS": "global_platform,global_ipm,*", "ENABLED": uint64(1),
						"REMARK": "规则里其中一项为'*'时，其它数据库排除在任意规则检查之外", "LOADED_STATE": "success", "LOADED_MESSAGE": "Loaded Success", "LOADED_DETAIL_ALLOW_ALL_DBS": uint64(0),
						"LOADED_DETAIL_SERVICE": "*", "LOADED_DETAIL_PACKAGE": "*", "LOADED_DETAIL_CROSS_DBS": "[]", "LOADED_DETAIL_FILTER_GLOBAL": uint64(1),
						"LOADED_DETAIL_FILTER_PATTERNS": "[global_ipm global_platform]",
					},
					{
						"RULE_ID": uint64(3), "INVOKER_NAME": "*", "INVOKER_TYPE": "both", "ALLOW_ALL_DBS": uint64(0), "CROSS_DBS": "global_dw_*,global_dwb,*", "ENABLED": uint64(1),
						"REMARK": "规则里其中一项为'*'时，其它数据库排除在任意规则检查之外", "LOADED_STATE": "success", "LOADED_MESSAGE": "Loaded Success", "LOADED_DETAIL_ALLOW_ALL_DBS": uint64(0),
						"LOADED_DETAIL_SERVICE": "*", "LOADED_DETAIL_PACKAGE": "*", "LOADED_DETAIL_CROSS_DBS": "[]", "LOADED_DETAIL_FILTER_GLOBAL": uint64(1),
						"LOADED_DETAIL_FILTER_PATTERNS": "[global_dw_* global_dwb]",
					},
					{
						"RULE_ID": uint64(4), "INVOKER_NAME": "@mctech/dp-impala-tidb-enhanced", "INVOKER_TYPE": "package", "ALLOW_ALL_DBS": uint64(1), "CROSS_DBS": "", "ENABLED": uint64(1),
						"REMARK": "删除约束检查里跨库约束规则检查，需要允许任意配置的跨库规则", "LOADED_STATE": "success", "LOADED_MESSAGE": "Loaded Success", "LOADED_DETAIL_ALLOW_ALL_DBS": uint64(1),
						"LOADED_DETAIL_SERVICE": "", "LOADED_DETAIL_PACKAGE": "@mctech/dp-impala-tidb-enhanced", "LOADED_DETAIL_CROSS_DBS": "[]", "LOADED_DETAIL_FILTER_GLOBAL": nil,
						"LOADED_DETAIL_FILTER_PATTERNS": nil,
					},
				}
				require.Equal(t, expect, rows)
			},
		},
		{
			sql: fmt.Sprintf("mctech show full_sql '%s' %d %d", datetime, 0, 1697003594435),
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 1)
				row := fetchResultsetFirstRow(t, rs)
				value := row["SQL_content"]
				require.Contains(t, value, "UPDATE gdcd_project_subcontract_bill_account")
			},
		},
		{
			sql: fmt.Sprintf("mctech show full_sql '%s' %d %d 'product'", datetime, 0, 1697003594437),
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 1)
				row := fetchResultsetFirstRow(t, rs)
				value := row["SQL_content"]
				require.Contains(t, value, "UPDATE gdcd_project_subcontract_bill_account")
			},
		},
		{
			sql: fmt.Sprintf("mctech show full_sql '%s' %d %d", datetime, 1, 10),
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 1)
				row := fetchResultsetFirstRow(t, rs)
				expect := map[string]any{"SQL_content": nil}
				require.Equal(t, expect, row)
			},
		},
		{
			sql: "mctech show dw_index",
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 1)
				row := fetchResultsetFirstRow(t, rs)
				value := row["Index_content"]
				require.Contains(t, value, `{"background":`)
			},
		},
		{
			sql: "mctech seq_decode 1310341421945856",
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 1)
				row := fetchResultsetFirstRow(t, rs)
				value := row["Seq_Decode_value"]
				require.Equal(t, "2022-07-07 14:16:41.964000", value)
			},
		},
		{
			sql: "mctech show deny_digest",
			check: func(rs sqlexec.RecordSet) {
				require.Len(t, rs.Fields(), 6)
				row := fetchResultsetFirstRow(t, rs)
				expect := map[string]any{
					"DIGEST":            "123456",
					"CREATED_AT":        "2026-01-05 12:31:05",
					"EXPIRED_AT":        nil,
					"LAST_REQUEST_TIME": "2026-02-15 02:11:05",
					"QUERY_SQL":         "select 1",
					"REMARK":            "this is a test",
				}
				require.Equal(t, expect, row)
			},
			mock: func(tk *testkit.TestKit) {
				tk.MustExec(fmt.Sprintf(
					`insert into mysql.%s
					(digest, created_at, expired_at, last_request_time, query_sql, remark)
					values ('123456', '2026-01-05 12:31:05', null, '2026-02-15 02:11:05', 'select 1', 'this is a test')
					`, worker.MCTechDenyDigest))
			},
		},
	}

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	for _, c := range cases {
		if c.mock != nil {
			c.mock(tk)
		}
		rs, err := tk.Exec(c.sql)
		require.NoError(t, err)
		c.check(rs)
	}
}

func TestBuildMCTechTenantEnabled(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Tenant.Enabled": true}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mock.M(t, map[string]any{"DWIndex.Current": map[string]any{"current": 1}}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")

	sql := "mctech SELECT * FROM t1"
	cases := []testBuildMCTechCase{
		{"demo-service", "", []string{"gslq"}, []string{"ys", "ys2"}, false, false, "", "", false, "", "1|[\"ys\", \"ys2\"]|[\"gslq\"]|{\"service\": \"demo-service\"}|<nil>|<nil>|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"mpp\": \"allow\"}|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf0", "", []string{"gslq", "mctech"}, []string{"ys2"}, false, false, "disable", "", false, "", "1|[\"ys2\"]|[\"gslq\", \"mctech\"]|{\"service\": \"demo-service.pf0\"}|<nil>|<nil>|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"mpp\": \"disable\"}|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf1", "@mctech/dp-impala", []string{"gslq", "mctech"}, nil, false, false, "force", "", false, "", "1|[]|[\"gslq\", \"mctech\"]|{\"pkg\": \"@mctech/dp-impala\", \"service\": \"demo-service.pf1\"}|<nil>|<nil>|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"mpp\": \"force\"} SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf2", "@mctech/dp-impala", nil, nil, false, false, "allow", "", true, "gslq", "0|[]|[]|{\"pkg\": \"@mctech/dp-impala\", \"service\": \"demo-service.pf2\"}|gslq|hint|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"gslq\"}|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf3", "@mctech/dp-impala", nil, nil, false, false, "", "code_sxlq", true, "", "0|[]|[]|{\"pkg\": \"@mctech/dp-impala\", \"service\": \"demo-service.pf3\"}|sxlq|role|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"sxlq\"}|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "", "0|[]|[]|{\"pkg\": \"@mctech/dp-impala\", \"service\": \"demo-service.pf4\"}|mctest|role|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"mctest\"}|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "mctest", "0|[]|[]|{\"pkg\": \"@mctech/dp-impala\", \"service\": \"demo-service.pf4\"}|mctest|role|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"mctest\"}|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		// tenant_omit
		{"demo-service.pf5", "", nil, []string{"ys", "ys2"}, false, true, "", "", false, "mctest", "1|[]|[]|{\"service\": \"demo-service.pf5\"}|<nil>|<nil>|%[1]s|[\"%[1]s\"]|[{\"db\": \"%[1]s\", \"table\": \"t1\"}]|{\"background\": 2, \"current\": 1}|{\"mpp\": \"allow\", \"tenant\": \"mctest\"}|SELECT * FROM `%[2]s`.`t1`%[3]s"},
	}

	dbs := []string{"global_pf", "public_data"}
	for _, db := range dbs {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf("create database %s", db))
		tk.MustExec(fmt.Sprintf("create table %s.t1 (tenant varchar(50), value double, primary key (tenant))", db))
		tk.MustExec(fmt.Sprintf("use %s", db))

		for _, c := range cases {
			testCase(t, tk, db, sql, true, &c)
		}
	}
}

func TestBuildMCTechTenantDisabled(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Tenant.Enabled": false}),
	)
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp",
		mock.M(t, map[string]any{"DWIndex.Current": map[string]any{"current": 2}}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/MockMctechHttp")

	sql := "mctech SELECT * FROM t1"
	cases := []testBuildMCTechCase{
		{"demo-service", "", []string{"gslq"}, []string{"ys2"}, false, false, "", "", false, "", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf0", "", []string{"gslq", "mctech"}, []string{"ys2"}, false, false, "disable", "", false, "", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf1", "@mctech/dp-impala", []string{"gslq", "mctech"}, nil, false, false, "force", "", false, "", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf2", "@mctech/dp-impala", nil, nil, false, false, "allow", "", true, "gslq", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf3", "@mctech/dp-impala", nil, nil, false, false, "", "code_sxlq", true, "", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "mctest", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
		// tenant_omit
		{"demo-service.pf5", "", nil, []string{"ys", "ys2"}, false, true, "", "", false, "mctest", "<nil>|<nil>|<nil>|<nil>|<nil>|<nil>|%[1]s|<nil>|<nil>|<nil>|<nil>|SELECT * FROM `%[2]s`.`t1`%[3]s"},
	}

	dbs := []string{"global_pf", "public_data"}
	for _, db := range dbs {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec(fmt.Sprintf("create database %s", db))
		tk.MustExec(fmt.Sprintf("create table %s.t1 (tenant varchar(50), value double, primary key (tenant))", db))
		tk.MustExec(fmt.Sprintf("use %s", db))

		for _, c := range cases {
			testCase(t, tk, db, sql, false, &c)
		}
	}
}

func testCase(t *testing.T, tk *testkit.TestKit, db string, sql string, tenantEnabled bool, c *testBuildMCTechCase) {
	var lst []string
	if c.from != "" {
		lst = append(lst, fmt.Sprintf("/* from:'%s' */", c.from))
	}
	if c.pkg != "" {
		lst = append(lst, fmt.Sprintf("/* package:'%s' */", c.pkg))
	}
	if c.mpp != "" {
		lst = append(lst, fmt.Sprintf("/*& mpp:%s */", c.mpp))
	}
	if c.impersonate {
		lst = append(lst, "/*& impersonate:'tenant_only' */")
	}

	roles := []string{}
	if c.tenantOnly {
		roles = append(roles, "tenant_only")
	}
	if c.tenantOmit {
		roles = append(roles, "tenant_omit")
	}

	var tenant string
	if c.tenantRole != "" {
		roles = append(roles, c.tenantRole)
		tenant = c.tenantRole[len("code_"):]
	}

	if tenant == "" {
		tenant = c.tenant
	} else {
		if c.tenant != "" {
			require.Equal(t, tenant, c.tenant, c.Source())
		}
	}

	var tenantCondition string
	if tenant == "" {
		if len(c.includes)+len(c.excludes) > 0 {
			globalItems := make([]string, 0, len(c.includes)+len(c.excludes))
			for _, include := range c.includes {
				globalItems = append(globalItems, "+"+include)
			}
			for _, exclude := range c.excludes {
				globalItems = append(globalItems, "-"+exclude)
			}
			lst = append(lst, fmt.Sprintf("/*& global:%s */", strings.Join(globalItems, ",")))
			if strings.HasPrefix(db, "global_") && !c.tenantOmit && tenantEnabled {
				var (
					includeCondition string
					excludeCondition string
				)
				if len(c.excludes) > 0 {
					items := []string{}
					for _, exclude := range c.excludes {
						items = append(items, fmt.Sprintf("_UTF8MB4'%s'", exclude))
					}
					excludeCondition = fmt.Sprintf("`t1`.`tenant` NOT IN (%s)", strings.Join(items, ","))
				}
				if len(c.includes) > 0 {
					items := []string{}
					for _, include := range c.includes {
						items = append(items, fmt.Sprintf("_UTF8MB4'%s'", include))
					}
					includeCondition = fmt.Sprintf("`t1`.`tenant` IN (%s)", strings.Join(items, ","))
				}
				if excludeCondition == "" {
					tenantCondition = " WHERE " + includeCondition
				} else if includeCondition == "" {
					tenantCondition = " WHERE " + excludeCondition
				} else {
					tenantCondition = " WHERE (" + excludeCondition + " AND " + includeCondition + ")"
				}
			}
		} else {
			lst = append(lst, "/*& global:true */")
		}
	} else {
		lst = append(lst, fmt.Sprintf("/*& tenant:'%s' */", tenant))
		if strings.HasPrefix(db, "global_") && !c.tenantOmit && tenant != "" && tenantEnabled {
			tenantCondition = fmt.Sprintf(" WHERE (`t1`.`tenant`=_UTF8MB4'%s')", tenant)
		}
	}

	if len(roles) > 0 {
		initSession(tk, "root", roles...)
	}
	lst = append(lst, sql)
	expected := fmt.Sprintf(c.expected, db, db, tenantCondition)
	res := tk.MustQuery(strings.Join(lst, "\n"))
	res.Check(testkit.RowsWithSep("|", expected))
}

func fetchResultsetRows(t *testing.T, rs sqlexec.RecordSet) []map[string]any {
	rawRows, err := sqlexec.DrainRecordSet(context.Background(), rs, 1024)
	require.NoError(t, err)
	rows := []map[string]any{}
	fields := rs.Fields()
	for _, rawRow := range rawRows {
		row := map[string]any{}
		for index, field := range fields {
			dt := rawRow.GetDatum(index, &field.Column.FieldType)
			var value any
			if !dt.IsNull() {
				v := dt.GetValue()
				switch x := v.(type) {
				case types.BinaryJSON:
					v = x.String()
				case types.Time:
					v = x.String()
				case types.Enum:
					v = x.String()
				}
				value = v
			}
			fieldName := field.Column.Name.O
			row[fieldName] = value
		}
		rows = append(rows, row)
	}
	return rows
}

func fetchResultsetFirstRow(t *testing.T, rs sqlexec.RecordSet) map[string]any {
	rows := fetchResultsetRows(t, rs)
	require.Len(t, rows, 1)
	return rows[0]
}
