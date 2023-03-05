package ddl

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

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
