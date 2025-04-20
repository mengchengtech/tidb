// add by zhangbing

package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/mctech/mock"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
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

func TestBuildMCTechTenantEnabled(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Tenant.Enabled": true}),
	)
	failpoint.Enable("github.com/pingcap/tidb/mctech/MockMctechHttp",
		mock.M(t, map[string]any{"DWIndex.Current": map[string]any{"current": 1}}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/MockMctechHttp")

	sql := "mctech SELECT * FROM t1"
	cases := []testBuildMCTechCase{
		{"demo-service", "", []string{"gslq"}, []string{"ys", "ys2"}, false, false, "", "", false, "", "1|ys,ys2|gslq|{demo-service,}|<nil>|<nil>|%s|1|{\"mpp\": \"allow\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf0", "", []string{"gslq", "mctech"}, []string{"ys2"}, false, false, "disable", "", false, "", "1|ys2|gslq,mctech|{demo-service.pf0,}|<nil>|<nil>|%s|1|{\"mpp\": \"disable\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf1", "@mctech/dp-impala", []string{"gslq", "mctech"}, nil, false, false, "force", "", false, "", "1||gslq,mctech|{demo-service.pf1,@mctech/dp-impala}|<nil>|<nil>|%s|1|{\"mpp\": \"force\"} SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf2", "@mctech/dp-impala", nil, nil, false, false, "allow", "", true, "gslq", "0|||{demo-service.pf2,@mctech/dp-impala}|gslq|hint|%s|1|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"gslq\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf3", "@mctech/dp-impala", nil, nil, false, false, "", "code_sxlq", true, "", "0|||{demo-service.pf3,@mctech/dp-impala}|sxlq|role|%s|1|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"sxlq\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "", "0|||{demo-service.pf4,@mctech/dp-impala}|mctest|role|%s|1|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"mctest\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "mctest", "0|||{demo-service.pf4,@mctech/dp-impala}|mctest|role|%s|1|{\"impersonate\": \"tenant_only\", \"mpp\": \"allow\", \"tenant\": \"mctest\"}|SELECT * FROM `%s`.`t1`%s"},
		// tenant_omit
		{"demo-service.pf5", "", nil, []string{"ys", "ys2"}, false, true, "", "", false, "mctest", "1|||{demo-service.pf5,}|<nil>|<nil>|%s|1|{\"mpp\": \"allow\", \"tenant\": \"mctest\"}|SELECT * FROM `%s`.`t1`%s"},
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
	failpoint.Enable("github.com/pingcap/tidb/config/GetMCTechConfig",
		mock.M(t, map[string]any{"Tenant.Enabled": false}),
	)
	failpoint.Enable("github.com/pingcap/tidb/mctech/MockMctechHttp",
		mock.M(t, map[string]any{"DWIndex.Current": map[string]any{"current": 2}}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/config/GetMCTechConfig")
	defer failpoint.Disable("github.com/pingcap/tidb/mctech/MockMctechHttp")

	sql := "mctech SELECT * FROM t1"
	cases := []testBuildMCTechCase{
		{"demo-service", "", []string{"gslq"}, []string{"ys2"}, false, false, "", "", false, "", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf0", "", []string{"gslq", "mctech"}, []string{"ys2"}, false, false, "disable", "", false, "", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf1", "@mctech/dp-impala", []string{"gslq", "mctech"}, nil, false, false, "force", "", false, "", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf2", "@mctech/dp-impala", nil, nil, false, false, "allow", "", true, "gslq", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf3", "@mctech/dp-impala", nil, nil, false, false, "", "code_sxlq", true, "", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", nil, nil, true, false, "allow", "code_mctest", true, "mctest", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
		// tenant_omit
		{"demo-service.pf5", "", nil, []string{"ys", "ys2"}, false, true, "", "", false, "mctest", "<nil>|||{}|<nil>|<nil>|%s|<nil>|{}|SELECT * FROM `%s`.`t1`%s"},
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
