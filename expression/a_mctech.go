package expression

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/mctech/udf"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"

	driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	_ functionClass = &mctechSequenceFunctionClass{}
	_ functionClass = &mctechVersionJustPassFunctionClass{}
	_ functionClass = &mctechDecryptFunctionClass{}
	_ functionClass = &mctechEncryptFunctionClass{}
	_ functionClass = &mctechSequenceDecodeFunctionClass{}
)
var (
	_ builtinFunc = &builtinMCTechSequenceSig{}
	_ builtinFunc = &builtinMCTechVersionJustPassSig{}
	_ builtinFunc = &builtinMCTechDecryptSig{}
	_ builtinFunc = &builtinMCTechEncryptSig{}
	_ builtinFunc = &builtinMCTechSequenceDecodeSig{}
)

func init() {
	DeferredFunctions[ast.MCTechSequence] = struct{}{}

	// mctech function.
	funcs[ast.MCSeq] = &mctechSequenceFunctionClass{baseFunctionClass{ast.MCTechSequence, 0, 0}}
	funcs[ast.MCVersionJustPass] = &mctechVersionJustPassFunctionClass{baseFunctionClass{ast.MCTechVersionJustPass, 0, 0}}
	funcs[ast.MCDecrypt] = &mctechDecryptFunctionClass{baseFunctionClass{ast.MCTechDecrypt, 1, 1}}
	funcs[ast.MCEncrypt] = &mctechEncryptFunctionClass{baseFunctionClass{ast.MCTechEncrypt, 1, 1}}
	funcs[ast.MCSeqDecode] = &mctechSequenceDecodeFunctionClass{baseFunctionClass{ast.MCTechSequenceDecode, 1, 1}}

	funcs[ast.MCTechSequence] = &mctechSequenceFunctionClass{baseFunctionClass{ast.MCTechSequence, 0, 0}}
	funcs[ast.MCTechVersionJustPass] = &mctechVersionJustPassFunctionClass{baseFunctionClass{ast.MCTechVersionJustPass, 0, 0}}
	funcs[ast.MCTechDecrypt] = &mctechDecryptFunctionClass{baseFunctionClass{ast.MCTechDecrypt, 1, 1}}
	funcs[ast.MCTechEncrypt] = &mctechEncryptFunctionClass{baseFunctionClass{ast.MCTechEncrypt, 1, 1}}
	funcs[ast.MCTechSequenceDecode] = &mctechSequenceDecodeFunctionClass{baseFunctionClass{ast.MCTechSequenceDecode, 1, 1}}

	// mctech function.
	unFoldableFunctions[ast.MCTechSequence] = struct{}{}
}

type mctechSequenceDecodeFunctionClass struct {
	baseFunctionClass
}

func (c *mctechSequenceDecodeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinMCTechSequenceDecodeSig{bf}
	bf.tp.SetFlen(mysql.MaxDatetimeFullWidth)
	return sig, nil
}

type builtinMCTechSequenceDecodeSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechSequenceDecodeSig) Clone() builtinFunc {
	newSig := &builtinMCTechSequenceDecodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMCTechSequenceDecodeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	id, isNull, err := b.args[0].EvalInt(b.ctx, row)

	if err != nil {
		return types.ZeroTime, true, err
	}

	if isNull {
		return types.ZeroTime, true, errors.New("sequence not allow null")
	}

	result, err := udf.SequenceDecode(id)
	if err != nil {
		return types.ZeroTime, true, err
	}

	dt := types.NewTime(types.FromGoTime(time.UnixMilli(result)),
		mysql.TypeTimestamp, 0)
	return dt, false, nil
}

// --------------------------------------------------------------

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

// --------------------------------------------------------------

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

// --------------------------------------------------------------

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

// --------------------------------------------------------------

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

// --------------------------------------------------------------

// IsValidMCTechSequenceExpr returns true if exprNode is a valid MCTechSequence expression.
// Here `valid` means it is consistent with the given fieldType's decimal.
func IsValidMCTechSequenceExpr(exprNode ast.ExprNode, fieldType *types.FieldType) bool {
	fn, isFuncCall := exprNode.(*ast.FuncCallExpr)
	if !isFuncCall || fn.FnName.L != ast.MCTechSequence {
		return false
	}
	return true
}

// GetNextSequence function
func GetNextSequence() (int64, error) {
	return udf.GetCache().Next()
}

// GetBigIntValue gets the time value with type tp.
func GetBigIntValue(ctx sessionctx.Context, v interface{}, tp byte, fsp int) (d types.Datum, err error) {
	var value int64
	switch x := v.(type) {
	case string:
		upperX := strings.ToUpper(x)
		if upperX == strings.ToUpper(ast.MCTechSequence) {
			if value, err = GetNextSequence(); err != nil {
				return d, err
			}
		} else {
			if value, err = strconv.ParseInt(x, 10, 64); err != nil {
				return d, err
			}
		}
	case *driver.ValueExpr:
		switch x.Kind() {
		case types.KindString:
			if value, err = strconv.ParseInt(x.GetString(), 10, 64); err != nil {
				return d, err
			}
		case types.KindInt64:
			value = x.GetInt64()
		case types.KindNull:
			return d, nil
		default:
			return d, errDefaultValue
		}
	case *ast.FuncCallExpr:
		if x.FnName.L == ast.MCTechSequence {
			d.SetString(strings.ToUpper(ast.MCTechSequence), mysql.DefaultCollationName)
			return d, nil
		}
		return d, errDefaultValue
	case *ast.UnaryOperationExpr:
		// support some expression, like `-1`
		v, err := EvalAstExpr(ctx, x)
		if err != nil {
			return d, err
		}
		ft := types.NewFieldType(mysql.TypeLonglong)
		xval, err := v.ConvertTo(ctx.GetSessionVars().StmtCtx, ft)
		if err != nil {
			return d, err
		}

		value = xval.GetInt64()
	default:
		return d, nil
	}
	d.SetInt64(value)
	return d, nil
}
