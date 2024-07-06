// add by zhangbing

package executor

import "github.com/pingcap/tidb/pkg/parser/ast"

func init() {
	funcName2Alias["mctech_sequence"] = ast.MCTechSequence
	funcName2Alias["mctech_version_just_pass"] = ast.MCTechVersionJustPass
	funcName2Alias["mctech_decrypt"] = ast.MCTechDecrypt
	funcName2Alias["mctech_encrypt"] = ast.MCTechEncrypt
}
