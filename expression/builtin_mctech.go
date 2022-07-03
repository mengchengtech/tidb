// add by zhangbing

package expression

import (
	"github.com/pingcap/tidb/mctech/udf"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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

type mctechSequenceFunctionClass struct {
	baseFunctionClass
}

func (c *mctechSequenceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

func (b *builtinMCTechSequenceSig) evalInt(row chunk.Row) (int64, bool, error) {
	v, err := udf.GetCache().Next()
	if err != nil {
		return 0, true, err
	}
	return v, false, nil
}

type mctechVersionJustPassFunctionClass struct {
	baseFunctionClass
}

func (c *mctechVersionJustPassFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

func (b *builtinMCTechVersionJustPassSig) evalInt(row chunk.Row) (int64, bool, error) {
	v, err := udf.GetCache().VersionJustPass()
	if err != nil {
		return 0, true, err
	}
	return v, false, nil
}

type mctechDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *mctechDecryptFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

func (b *builtinMCTechDecryptSig) evalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
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

func (c *mctechEncryptFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

func (b *builtinMCTechEncryptSig) evalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	cipher, err := udf.GetClient().Encrypt(val)
	if err != nil {
		return "", true, err
	}

	return cipher, false, nil
}
