package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/mctech/mock"
	mcworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestReloadDenyDigests(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade", mock.M(t, "false"))
	failpoint.Enable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig",
		mock.M(t, map[string]bool{"SQLChecker.Enabled": true}),
	)
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/pkg/config/GetMCTechConfig")
		failpoint.Disable("github.com/pingcap/tidb/pkg/session/mctech-ddl-upgrade")
	}()

	session.RegisterMCTechUpgradeForTest("denyDigest", initMCTechDenyDigest)
	defer session.UnregisterMCTechUpgradeForTest("denyDigest")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	mgr, _ := dom.DenyDigestManager()

	info1 := mgr.Get("digest-1")
	require.Nil(t, info1)
	info2 := mgr.Get("digest-2")
	require.NotNil(t, info2)
	require.Equal(t, info2.ExpiredAt, time.Date(9999, 10, 1, 0, 0, 0, 0, time.Local))
}

func initMCTechDenyDigest(ctx context.Context, sctx sessiontypes.Session) (err error) {
	ctx = util.WithInternalSourceType(ctx, "initMCTechDenyDigest")
	args := []any{
		mysql.SystemDB, mcworker.MCTechDenyDigest,
	}
	if _, err = sctx.ExecuteInternal(ctx, mcworker.CreateMCTechDenyDigest, args...); err != nil {
		return err
	}

	_, err = sctx.ExecuteInternal(ctx, `insert into %n.%n
	(digest, created_at, expired_at, last_request_time, query_sql)
	values
	('digest-1', '2024-05-01', '2024-06-01', null, 'select 1')
	, ('digest-2', '2024-05-01', '9999-10-01', null, 'select 2')
	`, args...)
	return err
}
