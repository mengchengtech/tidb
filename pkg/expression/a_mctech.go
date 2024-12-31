// add by zhangbing

package expression

import (
	"github.com/pingcap/tidb/pkg/mctech/udf"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &mctechSequenceFunctionClass{}
	_ functionClass = &mctechVersionJustPassFunctionClass{}
	_ functionClass = &mctechDecryptFunctionClass{}
	_ functionClass = &mctechEncryptFunctionClass{}
)
var (
	_ builtinFunc = &builtinMCTechSequenceSig{}
	_ builtinFunc = &builtinMCTechVersionJustPassSig{}
	_ builtinFunc = &builtinMCTechDecryptSig{}
	_ builtinFunc = &builtinMCTechEncryptSig{}
)

var sequenceCache *udf.SequenceCache

func init() {
	sequenceCache = udf.GetCache()

	// mctech function.
	funcs[ast.MCTechSequence] = &mctechSequenceFunctionClass{baseFunctionClass{ast.MCTechSequence, 0, 0}}
	funcs[ast.MCTechVersionJustPass] = &mctechVersionJustPassFunctionClass{baseFunctionClass{ast.MCTechVersionJustPass, 0, 0}}
	funcs[ast.MCTechDecrypt] = &mctechDecryptFunctionClass{baseFunctionClass{ast.MCTechDecrypt, 1, 1}}
	funcs[ast.MCTechEncrypt] = &mctechEncryptFunctionClass{baseFunctionClass{ast.MCTechEncrypt, 1, 1}}

	// deferredFunctions集合中保存的函数允许延迟计算，在不影响执行计划时可延迟计算，好处是当最终结果不需要函数计算时，可省掉无效的中间计算过程，特别是对unFoldableFunctions类型函数
	deferredFunctions[ast.MCTechSequence] = struct{}{}
	deferredFunctions[ast.MCTechVersionJustPass] = struct{}{}

	// 不可折叠函数（一般用在projection中表函数表达式中，整个sql中只按sql字面出现次数调用还是返回结果中每一行都调用一次）
	unFoldableFunctions[ast.MCTechSequence] = struct{}{}
	unFoldableFunctions[ast.MCTechVersionJustPass] = struct{}{}

	// mutableEffectsFunctions集合中保存的函数名称sql中不缓存，每次（行）执行的结果可能都不一样
	mutableEffectsFunctions[ast.MCTechSequence] = struct{}{}
	mutableEffectsFunctions[ast.MCTechVersionJustPass] = struct{}{}
}

type mctechSequenceFunctionClass struct {
	baseFunctionClass
}

func (c *mctechSequenceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinMCTechSequenceSig{bf}
	bf.tp.SetFlen(21)
	return sig, nil
}

type builtinMCTechSequenceSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechSequenceSig) Clone() builtinFunc {
	newSig := &builtinMCTechSequenceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (*builtinMCTechSequenceSig) evalInt(_ EvalContext, _ chunk.Row) (int64, bool, error) {
	v, err := sequenceCache.Next()
	if err != nil {
		return 0, true, err
	}
	return v, false, nil
}

type mctechVersionJustPassFunctionClass struct {
	baseFunctionClass
}

func (c *mctechVersionJustPassFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinMCTechVersionJustPassSig{bf}
	bf.tp.SetFlen(21)
	return sig, nil
}

type builtinMCTechVersionJustPassSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechVersionJustPassSig) Clone() builtinFunc {
	newSig := &builtinMCTechVersionJustPassSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (*builtinMCTechVersionJustPassSig) evalInt(_ EvalContext, _ chunk.Row) (int64, bool, error) {
	v, err := sequenceCache.VersionJustPass()
	if err != nil {
		return 0, true, err
	}
	return v, false, nil
}

type mctechDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *mctechDecryptFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	sig := &builtinMCTechDecryptSig{bf}
	bf.tp.SetFlen(mysql.MaxFieldCharLength)
	return sig, nil
}

type builtinMCTechDecryptSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechDecryptSig) Clone() builtinFunc {
	newSig := &builtinMCTechDecryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMCTechDecryptSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	plain, err := udf.GetClient().Decrypt(val)
	if err != nil {
		return "", true, err
	}

	return plain, false, nil
}

type mctechEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *mctechEncryptFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxFieldCharLength)
	sig := &builtinMCTechEncryptSig{bf}
	return sig, nil
}

type builtinMCTechEncryptSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechEncryptSig) Clone() builtinFunc {
	newSig := &builtinMCTechEncryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMCTechEncryptSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	cipher, err := udf.GetClient().Encrypt(val)
	if err != nil {
		return "", true, err
	}

	return cipher, false, nil
}
