// add by zhangbing

package session

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	mcworker "github.com/pingcap/tidb/mctech/worker"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

var mctechUpgradeForTest = map[string]func(Session, int64){}

// RegisterMCTechUpgradeForTest register upgrade for system table and data
func RegisterMCTechUpgradeForTest(name string, fn func(context.Context, Session) error) {
	if !intest.InTest {
		panic(errors.New("only supported in testing"))
	}
	mctechUpgradeForTest[name] = func(s Session, _ int64) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(internalSQLTimeout)*time.Second)
		err := fn(ctx, s)
		defer cancel()
		if err != nil {
			logutil.BgLogger().Fatal("mustExecute error", zap.Error(err), zap.Stack("stack"))
		}
	}
}

// UnregisterMCTechUpgradeForTest unregister upgrade for system table and data
func UnregisterMCTechUpgradeForTest(name string) {
	if !intest.InTest {
		panic(errors.New("only supported in testing"))
	}
	delete(mctechUpgradeForTest, name)
}

const (
	mcVersionKey = "mctech_extension_version"

	mctechVersion10001 = 10001
	mctechVersion10002 = 10002

	currentMCTechVersion int64 = mctechVersion10002
)

var mctechUpgradeVersion = []func(Session, int64){
	mctechUpgradeToVer10001,
	mctechUpgradeToVer10002,
}

// mctechUpgrade init mctech ddl
func mctechUpgrade(s Session) {
	if intest.InTest {
		var shouldInit bool
		failpoint.Inject("mctech-ddl-upgrade", func(_ failpoint.Value) {
			shouldInit = true
		})
		if !shouldInit {
			return
		}
	}

	if ver := getMCTechVersion(s); ver < currentMCTechVersion {
		logutil.BgLogger().Info("update mctech version. waiting......", zap.Int64("from", ver), zap.Int64("to", currentMCTechVersion))
		for _, upgrade := range mctechUpgradeVersion {
			upgrade(s, ver)
		}
		if intest.InTest {
			for _, upgrade := range mctechUpgradeForTest {
				upgrade(s, ver)
			}
		}
		updateMCTechVersion(s)
		logutil.BgLogger().Info("update mctech version", zap.String("state", "success"))
	}
}

func mctechUpgradeToVer10001(s Session, ver int64) {
	if ver >= mctechVersion10001 {
		return
	}
	// Create [mcworker.MCTechDenyDigest] table.
	mustExecute(s, mcworker.CreateMCTechDenyDigest, mysql.SystemDB, mcworker.MCTechDenyDigest)
}

func mctechUpgradeToVer10002(s Session, ver int64) {
	if ver >= mctechVersion10002 {
		return
	}
	// Create [mcworker.MCTechCrossDB] table.
	mustExecute(s, mcworker.CreateMCTechCrossDB, mysql.SystemDB, mcworker.MCTechCrossDB)
}

func updateMCTechVersion(s Session) {
	sql := "replace mysql.tidb (variable_name, variable_value, `comment`) values (%?, %?, 'MCTech extension version. Do not delete.')"
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(internalSQLTimeout)*time.Second)
	ctx = kv.WithInternalSourceType(ctx, "InternalMCTechDDL")
	_, err := s.ExecuteInternal(ctx, sql, mcVersionKey, currentMCTechVersion)
	defer cancel()
	if err != nil {
		logutil.BgLogger().Fatal("update mctech version", zap.String("state", "error"), zap.String("sql", sql), zap.Error(err))
		panic(err)
	}
}

func getMCTechVersion(s Session) int64 {
	var mctechVersion int64
	sql := "select variable_name, variable_value from mysql.tidb where variable_name = %?"
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(internalSQLTimeout)*time.Second)
	ctx = kv.WithInternalSourceType(ctx, "InternalMCTechDDL")
	rs, err := s.ExecuteInternal(ctx, sql, mcVersionKey)
	defer cancel()
	if err != nil {
		logutil.BgLogger().Fatal("query mctech version", zap.String("state", "error"), zap.String("sql", sql), zap.Error(err))
		panic(err)
	}

	if rs != nil {
		defer func() {
			terror.Log(rs.Close())
		}()
		rows, err := sqlexec.DrainRecordSet(context.TODO(), rs, 8)
		if err != nil {
			logutil.BgLogger().Fatal("fetch mctech version", zap.String("state", "error"), zap.Error(err))
		}

		if len(rows) > 0 {
			row := rows[0]
			value := row.GetString(1)
			if len(value) > 0 {
				if mctechVersion, err = strconv.ParseInt(value, 10, 64); err != nil {
					logutil.BgLogger().Fatal(fmt.Sprintf("'%s' must be positive integer", mcVersionKey), zap.String("state", "error"), zap.Error(err))
					panic(err)
				}
			}
		}
	}
	return mctechVersion
}

// CheckSQLDigest check sql digest is deny
func CheckSQLDigest(sctx sessionctx.Context, digest string) error {
	if sctx.GetSessionVars().InRestrictedSQL {
		return nil
	}

	dom := domain.GetDomain(sctx)
	var (
		mgr domain.DenyDigestManager
		ok  bool
	)
	if mgr, ok = dom.DenyDigestManager(); !ok {
		if !intest.InTest {
			return errors.New("Domain.denyDigestManager is nil")
		}
		return nil
	}

	var info *mcworker.DenyDigestInfo
	if info = mgr.Get(digest); info == nil {
		return nil
	}

	now := time.Now()
	info.LastRequestTime = &now
	if deny := now.Before(info.ExpiredAt); deny {
		return fmt.Errorf("current sql is rejected and resumed at '%s' . digest: %s", info.ExpiredAt.Format("2006-01-02 15:04:05.0000"), digest)
	}
	return nil
}
