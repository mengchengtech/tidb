// add by zhangbing

package session_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/mctech/digestworker"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCheckSQLDigest(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/mctech/digestworker/GetDenyDigestInfo", mock.M(t, "digest-2"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/mctech/digestworker/GetDenyDigestInfo")

	store := testkit.CreateMockStore(t)
	dom, _ := session.GetDomain(store)
	m := digestworker.NewDigestManager(nil)
	dom.SetDenyDigestManagerForTest(m)
	tk := testkit.NewTestKit(t, store)

	var err error
	se := tk.Session()
	err = session.CheckSQLDigest(se, "digest-1")
	require.NoError(t, err)

	err = session.CheckSQLDigest(se, "digest-2")
	require.Error(t, err, "current sql is rejected and resumed at")
}
