// add by zhangbing

package infosync

import (
	"context"
	"math"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// SyncTiFlashRulesMaxRetry sync tiflash rule max retry count
	SyncTiFlashRulesMaxRetry = 3
)

// NewTiFlashTableRules create tiflash rule from table. not contain the partition that has placement policy itself
func NewTiFlashTableRules(tbl *model.TableInfo, bundle *placement.Bundle) ([]*placement.TiFlashRule, error) {
	if tbl.TiFlashReplica == nil {
		// 跳过不存在tiflash的表
		return nil, nil
	}

	rules := []*placement.TiFlashRule{}
	if tbl.TiFlashReplica != nil {
		newRule := MakeNewRule(tbl.ID, tbl.TiFlashReplica.Count, tbl.TiFlashReplica.LocationLabels)
		if err := attachTiflshRuleFromBundle(tbl.ID, &newRule, bundle); err != nil {
			return nil, err
		}
		rules = append(rules, &newRule)

		if tbl.Partition != nil {
			for _, p := range tbl.Partition.Definitions {
				if p.PlacementPolicyRef != nil {
					// 此处不包含独立配置的分区，会单独处理
					continue
				}
				rule, err := NewTiFlashPartitionRule(p.ID, tbl, bundle)
				if err != nil {
					return nil, err
				}
				rules = append(rules, rule)
			}
		}
	}
	return rules, nil
}

// NewTiFlashPartitionRule create TiFlashRule from partition
func NewTiFlashPartitionRule(partID int64, tbl *model.TableInfo, bundle *placement.Bundle) (*placement.TiFlashRule, error) {
	if tbl.TiFlashReplica == nil {
		// 跳过不存在tiflash的表
		return nil, nil
	}

	newRule := MakeNewRule(partID, tbl.TiFlashReplica.Count, tbl.TiFlashReplica.LocationLabels)
	if bundle != nil {
		// bundle可能为nil，此时只需忽略即可
		// set placement policy = default; 就是删除policy
		if err := attachTiflshRuleFromBundle(partID, &newRule, bundle); err != nil {
			return nil, err
		}
	}
	return &newRule, nil
}

// PutTiFlashRulesWithDefaultRetry will retry for default times
func PutTiFlashRulesWithDefaultRetry(ctx context.Context, rules []*placement.TiFlashRule) (err error) {
	return PutTiFlashRulesWithRetry(ctx, rules, SyncTiFlashRulesMaxRetry, RequestRetryInterval)
}

// PutTiFlashRulesWithRetry will retry for specified times when PutRuleBundles failed
func PutTiFlashRulesWithRetry(ctx context.Context, rules []*placement.TiFlashRule, maxRetry int, interval time.Duration) (err error) {
	if maxRetry < 0 {
		maxRetry = 0
	}

	for i := 0; i <= maxRetry; i++ {
		if err = PutTiFlashRules(ctx, rules); err == nil || ErrHTTPServiceError.Equal(err) {
			return err
		}

		if i != maxRetry {
			logutil.BgLogger().Warn("Error occurs when PutRuleBundles, retry", zap.Error(err))
			time.Sleep(interval)
		}
	}

	return
}

// PutTiFlashRules is used to post specific tiflash rule to PD.
func PutTiFlashRules(ctx context.Context, rules []*placement.TiFlashRule) error {
	failpoint.Inject("putTiFlashRulesError", func(isServiceError failpoint.Value) {
		var err error
		if isServiceError.(bool) {
			err = ErrHTTPServiceError.FastGen("mock service error")
		} else {
			err = errors.New("mock other error")
		}
		failpoint.Return(err)
	})

	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}

	tiflashRules := make([]placement.TiFlashRule, 0, len(rules))
	for _, r := range rules {
		tiflashRules = append(tiflashRules, *r)
	}
	return is.tiflashReplicaManager.SetPlacementRuleBatch(ctx, tiflashRules)
}

var roleToPriorities = map[placement.PeerRoleType]uint8{
	placement.Learner:  0,
	placement.Follower: 1,
	placement.Voter:    2,
	placement.Leader:   3,
}

func attachTiflshRuleFromBundle(id int64, rule *placement.TiFlashRule, bundle *placement.Bundle) error {
	if len(bundle.Rules) == 0 {
		rule.LocationLabels = nil
		return nil
	}

	// 获取rule的优先级 Learner > Follower > Voter > Leader
	var (
		seletedRule *placement.Rule                 // 选中的放置规则
		priority    uint8           = math.MaxUint8 // 预设置一个大值
	)
	for _, r := range bundle.Rules {
		p := roleToPriorities[r.Role]
		if p < priority {
			priority = p
			seletedRule = r
		}
	}

	if seletedRule == nil {
		return errors.Errorf("[attachLeaderRuleFromBundle] groupId %s, leader rule doesn't exist in Bundle", placement.GroupID(id))
	}

	for _, c := range seletedRule.Constraints {
		if c.Key == "engine" {
			// 提取的Contraints需要排除key=engine的约束项
			continue
		}
		rule.Constraints = append(rule.Constraints, c)
	}
	rule.LocationLabels = slices.Clone(seletedRule.LocationLabels)
	return nil
}
