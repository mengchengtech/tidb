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
	"github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

const (
	// SyncTiFlashRulesMaxRetry sync tiflash rule max retry count
	SyncTiFlashRulesMaxRetry = 3
)

// NewTiFlashTableRules create tiflash rule from table. not contain the partition that has placement policy itself
func NewTiFlashTableRules(tbl *model.TableInfo, bundle *placement.Bundle) ([]*http.Rule, error) {
	if tbl.TiFlashReplica == nil {
		// 跳过不存在tiflash的表
		return nil, nil
	}

	rules := []*http.Rule{}
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
func NewTiFlashPartitionRule(partID int64, tbl *model.TableInfo, bundle *placement.Bundle) (*http.Rule, error) {
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
func PutTiFlashRulesWithDefaultRetry(ctx context.Context, rules []*http.Rule) (err error) {
	return PutTiFlashRulesWithRetry(ctx, rules, SyncTiFlashRulesMaxRetry, RequestRetryInterval)
}

// PutTiFlashRulesWithRetry will retry for specified times when PutRuleBundles failed
func PutTiFlashRulesWithRetry(ctx context.Context, rules []*http.Rule, maxRetry int, interval time.Duration) (err error) {
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
func PutTiFlashRules(ctx context.Context, rules []*http.Rule) error {
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

	// 8.1+ SetPlacementRuleBatch传入参数变成了指针列表，所以此处不再需要生成非指针的列表
	return is.tiflashReplicaManager.SetPlacementRuleBatch(ctx, rules)
}

var roleToPriorities = map[http.PeerRoleType]uint8{
	http.Learner:  0,
	http.Follower: 1,
	http.Voter:    2,
	http.Leader:   3,
}

func attachTiflshRuleFromBundle(id int64, rule *http.Rule, bundle *placement.Bundle) error {
	// 获取rule的优先级 Learner > Follower > Voter > Leader
	var (
		seletedRule *http.Rule                 // 选中的放置规则
		priority    uint8      = math.MaxUint8 // 预设置一个大值
	)
	for _, r := range bundle.Rules {
		p := roleToPriorities[r.Role]
		if p < priority {
			priority = p
			seletedRule = r
		}
	}

	if seletedRule == nil {
		return errors.Errorf("[attachLeaderRuleFromBundle] groupId %s, leader rule don't exists in Bundle", placement.GroupID(id))
	}

	for _, c := range seletedRule.LabelConstraints {
		if c.Key == "engine" {
			// 提取的Contraints需要排除key=engine的约束项
			continue
		}
		rule.LabelConstraints = append(rule.LabelConstraints, c)
	}
	rule.LocationLabels = slices.Clone(seletedRule.LocationLabels)
	return nil
}
