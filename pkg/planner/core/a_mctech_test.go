// add by zhangbing

package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type testBuildMCTechCase struct {
	from        string
	pkg         string
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
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"Tenant.Enabled": true}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	sql := "mctech SELECT * FROM t1"
	cases := []testBuildMCTechCase{
		{"demo-service", "", false, false, "", "", false, "", "1||{demo-service,}||none|%s|18446744073709551615|{\"mpp\":\"allow\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf0", "", false, false, "disable", "", false, "", "1||{demo-service.pf0,}||none|%s|18446744073709551615|{\"mpp\":\"disable\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf1", "@mctech/dp-impala", false, false, "force", "", false, "", "1||{demo-service.pf1,@mctech/dp-impala}||none|%s|18446744073709551615|{\"mpp\":\"force\"} SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf2", "@mctech/dp-impala", false, false, "allow", "", true, "gslq", "0||{demo-service.pf2,@mctech/dp-impala}|gslq|hint|%s|18446744073709551615|{\"impersonate\":\"tenant_only\",\"mpp\":\"allow\",\"tenant\":\"gslq\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf3", "@mctech/dp-impala", false, false, "", "code_sxlq", true, "", "0||{demo-service.pf3,@mctech/dp-impala}|sxlq|role|%s|18446744073709551615|{\"impersonate\":\"tenant_only\",\"mpp\":\"allow\",\"tenant\":\"sxlq\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", true, false, "allow", "code_mctest", true, "", "0||{demo-service.pf4,@mctech/dp-impala}|mctest|role|%s|18446744073709551615|{\"impersonate\":\"tenant_only\",\"mpp\":\"allow\",\"tenant\":\"mctest\"}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", true, false, "allow", "code_mctest", true, "mctest", "0||{demo-service.pf4,@mctech/dp-impala}|mctest|role|%s|18446744073709551615|{\"impersonate\":\"tenant_only\",\"mpp\":\"allow\",\"tenant\":\"mctest\"}|SELECT * FROM `%s`.`t1`%s"},
		// tenant_omit
		{"demo-service.pf5", "", false, true, "", "", false, "mctest", "1||{demo-service.pf5,}||none|%s|18446744073709551615|{\"mpp\":\"allow\",\"tenant\":\"mctest\"}|SELECT * FROM `%s`.`t1`%s"},
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
		mock.M(t, map[string]bool{"Tenant.Enabled": false}),
	)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")

	sql := "mctech SELECT * FROM t1"
	cases := []testBuildMCTechCase{
		{"demo-service", "", false, false, "", "", false, "", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf0", "", false, false, "disable", "", false, "", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf1", "@mctech/dp-impala", false, false, "force", "", false, "", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf2", "@mctech/dp-impala", false, false, "allow", "", true, "gslq", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf3", "@mctech/dp-impala", false, false, "", "code_sxlq", true, "", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", true, false, "allow", "code_mctest", true, "", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
		{"demo-service.pf4", "@mctech/dp-impala", true, false, "allow", "code_mctest", true, "mctest", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
		// tenant_omit
		{"demo-service.pf5", "", false, true, "", "", false, "mctest", "0||{}||none|%s|18446744073709551615|{}|SELECT * FROM `%s`.`t1`%s"},
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
		lst = append(lst, "/*& global:true */")
	} else {
		lst = append(lst, fmt.Sprintf("/*& tenant:'%s' */", tenant))
	}

	if strings.HasPrefix(db, "global_") && !c.tenantOmit && tenant != "" && tenantEnabled {
		tenantCondition = fmt.Sprintf(" WHERE (`t1`.`tenant`=_UTF8MB4'%s')", tenant)
	}

	if len(roles) > 0 {
		initSession(tk, "root", roles...)
	}
	lst = append(lst, sql)
	expected := fmt.Sprintf(c.expected, db, db, tenantCondition)
	res := tk.MustQuery(strings.Join(lst, "\n"))
	res.Check(testkit.RowsWithSep("|", expected))
}
