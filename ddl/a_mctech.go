// add by zhangbing

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type innerPartInfo struct {
	ID      int64
	tblInfo *model.TableInfo
}

func setMCTechSequenceDefaultValue(c *table.Column, hasDefaultValue bool, setOnUpdateNow bool) {
	if hasDefaultValue || mysql.HasAutoIncrementFlag(c.GetFlag()) {
		return
	}

	// For mctech_sequence Col, if is not set default value or not set null, use current timestamp.
	if c.GetType() == mysql.TypeLonglong && mysql.HasNotNullFlag(c.GetFlag()) {
		if setOnUpdateNow {
			if err := c.SetDefaultValue("0"); err != nil {
				logutil.BgLogger().Error("set default value failed", zap.Error(err))
			}
		}
		// else {
		// 	if err := c.SetDefaultValue(strings.ToUpper(ast.MCTechSequence)); err != nil {
		// 		logutil.BgLogger().Error("set default value failed", zap.Error(err))
		// 	}
		// }
	}
}

func updateExistTiFlashPlacementPolicy(tblInfos []*model.TableInfo, partInfos []*innerPartInfo, bundle *placement.Bundle) error {
	rules := make([]*placement.TiFlashRule, 0, len(tblInfos)+len(partInfos))
	for _, tbl := range tblInfos {
		if tbl.TiFlashReplica == nil {
			// 当前表不存在tiflash replica。忽略
			continue
		}
		// 表级的Placement Policy，如果存在分区，也包含分区默认的Policy（不含独立配置Policy的分区）
		tblRules := infosync.NewTiFlashTableRules(tbl, bundle)
		rules = append(rules, tblRules...)
	}

	for _, part := range partInfos {
		if part.tblInfo.TiFlashReplica == nil {
			// 当前分区所属的表没有tiflash replica。忽略
			continue
		}

		partRule := infosync.NewTiFlashPartitionRule(part.ID, part.tblInfo, bundle)
		rules = append(rules, partRule)
	}

	if len(rules) > 0 {
		err := infosync.PutTiFlashRulesWithDefaultRetry(context.TODO(), rules)
		if err != nil {
			return errors.Wrapf(err, "failed to notify PD the tiflash placement rules")
		}
	}

	return nil
}

// updateTiflashTableReplacementPolicy
// bundles 包含了table和所有继承自table placement的 partitions
func updateTiflashTableReplacementPolicy(tblInfo *model.TableInfo, bundle *placement.Bundle) (err error) {
	// 修改表的replacement poLicy配置信息时。
	// 修改表的policy会影响到继承自表分区的policy，但是又不会影响独立设置过放置信息的分区
	// 因此时修改表的Policy时，需要把独立配置了Policy的分区排除掉

	if tblInfo.TiFlashReplica == nil {
		return nil
	}

	// 表级的Placement Policy，如果存在分区，也包含分区默认的Policy（不含独立配置Policy的分区）
	rules := infosync.NewTiFlashTableRules(tblInfo, bundle)
	if err == nil && len(rules) > 0 {
		err = infosync.PutTiFlashRulesWithDefaultRetry(context.TODO(), rules)
	}

	return err
}

// updateNewTiflashTablePartitionsReplacementPolicy update new add partitions
func updateNewTiflashTablePartitionsReplacementPolicy(tblInfo *model.TableInfo, definitions []model.PartitionDefinition, bundles []*placement.Bundle) error {
	if tblInfo.TiFlashReplica == nil {
		return nil
	}

	rules := make([]*placement.TiFlashRule, 0, len(definitions))

	for _, p := range definitions {
		bundle := findBundle(bundles, p.ID)
		if bundle != nil {
			// 分区存在独立的placement，添加新建的分区的placement policy的放置策略
			rule := infosync.NewTiFlashPartitionRule(p.ID, tblInfo, bundle)
			if rule != nil {
				rules = append(rules, rule)
			}
		}
	}
	return infosync.PutTiFlashRulesWithDefaultRetry(context.TODO(), rules)
}

func updateTiflashTablePartitionReplacementPolicy(partitionID int64, tblInfo *model.TableInfo, bundle *placement.Bundle) error {
	// 添加分区的placement policy的放置策略
	rule := infosync.NewTiFlashPartitionRule(partitionID, tblInfo, bundle)
	if rule != nil {
		return infosync.PutTiFlashRulesWithDefaultRetry(context.TODO(), []*placement.TiFlashRule{rule})
	}
	return nil
}

// updateFullTiflashTableReplacementPolicy
// bundles 包含了table和所有partitions （独立配置和继承自table的placement）
func updateFullTiflashTableReplacementPolicy(tblInfo *model.TableInfo, bundles []*placement.Bundle) (err error) {
	if tblInfo.TiFlashReplica == nil {
		return nil
	}

	var rules []*placement.TiFlashRule
	bundle := findBundle(bundles, tblInfo.ID)
	if bundle != nil {
		rules = infosync.NewTiFlashTableRules(tblInfo, bundle)
	}

	if tblInfo.Partition != nil {
		allDefinitions := []model.PartitionDefinition{}
		allDefinitions = append(allDefinitions, tblInfo.Partition.Definitions...)
		allDefinitions = append(allDefinitions, tblInfo.Partition.AddingDefinitions...)

		for _, p := range allDefinitions {
			partBundle := bundle
			if p.PlacementPolicyRef != nil {
				// 找出当前分区的Policy
				b := findBundle(bundles, tblInfo.ID)
				if b != nil {
					// 如果存在，使用分区的Policy，否则使用表上的Policy
					partBundle = b
				}
			}
			if partBundle != nil {
				rule := infosync.NewTiFlashPartitionRule(p.ID, tblInfo, partBundle)
				rules = append(rules, rule)
			}
		}
	}

	if err == nil && len(rules) > 0 {
		err = infosync.PutTiFlashRulesWithDefaultRetry(context.TODO(), rules)
	}

	return err
}

func findBundle(bundles []*placement.Bundle, id int64) *placement.Bundle {
	groupId := placement.GroupID(id)
	for _, b := range bundles {
		if b.ID == groupId {
			return b
		}
	}
	return nil
}
