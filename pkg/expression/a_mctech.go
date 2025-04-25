// add by zhangbing

package expression

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/mctech/udf"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &mctechSequenceFunctionClass{}
	_ functionClass = &mctechVersionJustPassFunctionClass{}
	_ functionClass = &mctechDecryptFunctionClass{}
	_ functionClass = &mctechEncryptFunctionClass{}
	_ functionClass = &mctechSequenceDecodeFunctionClass{}
	_ functionClass = &mctechGetFullSQLFunctionClass{}
	_ functionClass = &mctechDataWarehouseIndexInfoFunctionClass{}
)
var (
	_ builtinFunc = &builtinMCTechSequenceSig{}
	_ builtinFunc = &builtinMCTechVersionJustPassSig{}
	_ builtinFunc = &builtinMCTechDecryptSig{}
	_ builtinFunc = &builtinMCTechDecryptAndMaskSig{}
	_ builtinFunc = &builtinMCTechEncryptSig{}
	_ builtinFunc = &builtinMCTechSequenceDecodeSig{}
	_ builtinFunc = &builtinMCTechGetFullSQLSig{}
	_ builtinFunc = &builtinMCTechDataWarehouseIndexInfoSig{}
)

type parameterInfo struct {
	name        string
	tp          string
	description string
}

type returnInfo struct {
	tp          string
	description string
}

type signatureInfo struct {
	parameters  []parameterInfo
	returnType  returnInfo
	description string
}

type mctechFunctionInfo struct {
	name       string
	shortName  string
	mutable    bool
	hidden     bool
	signatures []signatureInfo
}

var mctechFunctionHelps = []mctechFunctionInfo{
	{
		name: "mctech_sequence", shortName: "mc_seq", mutable: true,
		signatures: []signatureInfo{
			{
				description: "获取主键序列，与程序框架里获取主键序列的算法和来源相同",
				returnType: returnInfo{
					tp:          "bigint",
					description: "即时生成的主键序列值。生成的值里包含了当前的时间信息，可通过mctech_sequence_decode函数解析出时间信息。",
				},
			},
		},
	},
	{
		hidden: true,
		name:   "mctech_version_just_pass", shortName: "mc_version_just_pass",
		signatures: []signatureInfo{
			{
				description: "获取一个比当前主键序列值的时间略小一点（目前是3秒）的值，一般用于查询中version过滤条件",
				returnType: returnInfo{
					tp:          "bigint",
					description: "即时生成的一个version值",
				},
			},
		},
	},
	{
		name: "mctech_decrypt", shortName: "mc_decrypt", mutable: true,
		signatures: []signatureInfo{
			{
				description: "解密自定义算法加密的字符串",
				parameters:  []parameterInfo{{name: "cipher", tp: "string", description: "要解密的字符串"}},
				returnType: returnInfo{
					tp:          "string",
					description: "解密后原始的字符串",
				},
			},
			{
				description: "解密自定义算法加密的字符串，并且使用内置的mask字符替换原始字符串中指定位置和长度的字符，隐藏敏感信息",
				parameters: []parameterInfo{
					{name: "cipher", tp: "string", description: "要解密的字符串"},
					{name: "from", tp: "int", description: "需要替换为内置字符的起始位置（从1开始）"},
					{name: "length", tp: "int", description: "需要替换的长度"},
				},
				returnType: returnInfo{
					tp:          "string",
					description: "解密后被内置mask字符替换后的字符串。内置mask字符为'*'。当调用方式为 ('.....', 4, 4) 时，如果解码后的原始字符串为'13812345678'，实际返回的结果为 '138****5678'",
				},
			},
			{
				description: "解密自定义算法加密的字符串，并且使用传入的mask字符替换原始字符串中指定位置和长度的字符，隐藏敏感信息",
				parameters: []parameterInfo{
					{name: "cipher", tp: "string", description: "要解密的字符串"},
					{name: "from", tp: "int", description: "需要替换为内置字符的起始位置（从1开始）"},
					{name: "length", tp: "int", description: "需要替换的长度"},
					{name: "mask", tp: "char", description: "用给定的字符代码内置的默认字符执行替换"},
				},
				returnType: returnInfo{
					tp:          "string",
					description: "解密后被给定mask字符替换后的字符串。当调用方式为 ('.....', 4, 4, '#') 时，如果解码后的原始字符串为'13812345678'，实际返回的结果为 '138####5678'",
				},
			},
		},
	},
	{
		name: "mctech_encrypt", shortName: "mc_encrypt", mutable: true,
		signatures: []signatureInfo{{
			description: "用自定义加密算法加密给定的字符串",
			parameters:  []parameterInfo{{name: "plain", tp: "string", description: "需要加密的字符串"}},
			returnType: returnInfo{
				tp:          "string",
				description: "加密后的字符串，以'{cipher}'为前缀",
			},
		}},
	},
	{
		name: "mctech_sequence_decode", shortName: "mc_seq_decode", mutable: true,
		signatures: []signatureInfo{{
			description: "从序列值中提取时间信息，可用于了解生成该序列值的具体时间",
			parameters:  []parameterInfo{{name: "seq", tp: "bigint", description: "由 mctech_sequence 方法，或代码框架中等效调用方式生成的序列值"}},
			returnType: returnInfo{
				tp:          "datetime",
				description: "序列中包含的时间信息。例如传入 1712497548663304 返回值为 '2024-01-26 18:45:26'",
			},
		}},
	},
	{
		name: "mctech_get_full_sql", shortName: "mc_get_full_sql", mutable: true,
		signatures: []signatureInfo{{
			description: "获取sql执行信息中的保存在磁盘上的完整sql。仅限于在导入过sql执行信息的数据库上使用",
			parameters: []parameterInfo{
				{name: "at", tp: "datetime|string", description: "sql执行信息中'at'字段的值。"},
				{name: "txId", tp: "bigint", description: "sql执行信息中tx_id字段的值。"},
			},
			returnType: returnInfo{
				tp:          "string",
				description: "从磁盘上加载的完整sql。如果磁盘上找不到，则返回null",
			},
		}},
	},
	{
		name: "mctech_get_data_warehouse_index_info", shortName: "mc_dw_index_info", mutable: false,
		signatures: []signatureInfo{{
			description: "获取数仓前后台库索引信息",
			returnType: returnInfo{
				tp:          "json",
				description: "返回结果为固定结构的json格式，类似于 {\"current\":1,\"background\":2}。其中 current表示当前正在使用的dw库索引，background表示后台dw库索引",
			},
		}},
	},
}

func init() {
	// mctech function.
	funcs[ast.MCSeq] = &mctechSequenceFunctionClass{baseFunctionClass{ast.MCSeq, 0, 0}}
	funcs[ast.MCVersionJustPass] = &mctechVersionJustPassFunctionClass{baseFunctionClass{ast.MCVersionJustPass, 0, 0}}
	funcs[ast.MCDecrypt] = &mctechDecryptFunctionClass{baseFunctionClass{ast.MCDecrypt, 1, 4}}
	funcs[ast.MCEncrypt] = &mctechEncryptFunctionClass{baseFunctionClass{ast.MCEncrypt, 1, 1}}
	funcs[ast.MCSeqDecode] = &mctechSequenceDecodeFunctionClass{baseFunctionClass{ast.MCSeqDecode, 1, 1}}
	funcs[ast.MCGetFullSql] = &mctechGetFullSQLFunctionClass{baseFunctionClass{ast.MCGetFullSql, 2, 3}}
	funcs[ast.MCDWIndexInfo] = &mctechDataWarehouseIndexInfoFunctionClass{baseFunctionClass{ast.MCDWIndexInfo, 0, 0}}
	funcs[ast.MCHelp] = &mctechHelpFunctionClass{baseFunctionClass{ast.MCHelp, 0, 0}}

	funcs[ast.MCTechSequence] = &mctechSequenceFunctionClass{baseFunctionClass{ast.MCTechSequence, 0, 0}}
	funcs[ast.MCTechVersionJustPass] = &mctechVersionJustPassFunctionClass{baseFunctionClass{ast.MCTechVersionJustPass, 0, 0}}
	funcs[ast.MCTechDecrypt] = &mctechDecryptFunctionClass{baseFunctionClass{ast.MCTechDecrypt, 1, 4}}
	funcs[ast.MCTechEncrypt] = &mctechEncryptFunctionClass{baseFunctionClass{ast.MCTechEncrypt, 1, 1}}
	funcs[ast.MCTechSequenceDecode] = &mctechSequenceDecodeFunctionClass{baseFunctionClass{ast.MCTechSequenceDecode, 1, 1}}
	funcs[ast.MCTechGetFullSql] = &mctechGetFullSQLFunctionClass{baseFunctionClass{ast.MCTechGetFullSql, 2, 3}}
	funcs[ast.MCTechDataWarehouseIndexInfo] = &mctechDataWarehouseIndexInfoFunctionClass{baseFunctionClass{ast.MCDWIndexInfo, 0, 0}}
	funcs[ast.MCTechHelp] = &mctechHelpFunctionClass{baseFunctionClass{ast.MCHelp, 0, 0}}

	// deferredFunctions集合中保存的函数允许延迟计算，在不影响执行计划时可延迟计算，好处是当最终结果不需要函数计算时，可省掉无效的中间计算过程，特别是对unFoldableFunctions类型函数
	deferredFunctions[ast.MCTechSequence] = struct{}{}
	deferredFunctions[ast.MCTechVersionJustPass] = struct{}{}
	deferredFunctions[ast.MCSeq] = struct{}{}
	deferredFunctions[ast.MCVersionJustPass] = struct{}{}
	deferredFunctions[ast.MCDWIndexInfo] = struct{}{}

	// 不可折叠函数（一般用在projection中表函数表达式中，整个sql中只按sql字面出现次数调用还是返回结果中每一行都调用一次）
	unFoldableFunctions[ast.MCTechSequence] = struct{}{}
	unFoldableFunctions[ast.MCTechVersionJustPass] = struct{}{}
	unFoldableFunctions[ast.MCSeq] = struct{}{}
	unFoldableFunctions[ast.MCVersionJustPass] = struct{}{}
	unFoldableFunctions[ast.MCDWIndexInfo] = struct{}{}

	// mutableEffectsFunctions集合中保存的函数名称sql中不缓存，每次（行）执行的结果可能都不一样
	mutableEffectsFunctions[ast.MCTechSequence] = struct{}{}
	mutableEffectsFunctions[ast.MCTechVersionJustPass] = struct{}{}
	mutableEffectsFunctions[ast.MCSeq] = struct{}{}
	mutableEffectsFunctions[ast.MCVersionJustPass] = struct{}{}
	mutableEffectsFunctions[ast.MCDWIndexInfo] = struct{}{}
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

func (*builtinMCTechSequenceSig) evalInt(_ chunk.Row) (int64, bool, error) {
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

func (*builtinMCTechVersionJustPassSig) evalInt(_ chunk.Row) (int64, bool, error) {
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
	var (
		argTps []types.EvalType
		sig    builtinFunc
	)
	length := len(args) // 参数个数
	switch length {
	case 1:
		argTps = append(argTps, types.ETString)
	case 3:
		argTps = append(argTps, types.ETString, types.ETInt, types.ETInt)
	case 4:
		argTps = append(argTps, types.ETString, types.ETInt, types.ETInt, types.ETString)
	default:
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs("mc_decrypt")
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	switch {
	case length == 1:
		sig = &builtinMCTechDecryptSig{bf}
	case length >= 3:
		sig = &builtinMCTechDecryptAndMaskSig{bf}
	default:
		// Should never happens.
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs("mc_decrypt")
	}
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

var maskStrs = []string{}

func init() {
	for i := 0; i < 20; i++ {
		maskStrs = append(maskStrs, strings.Repeat("*", i))
	}
}

type builtinMCTechDecryptAndMaskSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechDecryptAndMaskSig) Clone() builtinFunc {
	newSig := &builtinMCTechDecryptAndMaskSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMCTechDecryptAndMaskSig) evalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	var (
		maskFrom   int64
		maskLength int64
		maskChar   = ""
	)
	if maskFrom, isNull, err = b.args[1].EvalInt(b.ctx, row); isNull || err != nil {
		return "", isNull, err
	}

	if maskLength, isNull, err = b.args[2].EvalInt(b.ctx, row); isNull || err != nil {
		return "", isNull, err
	}

	if len(b.args) == 4 {
		// 传入替换的char
		if maskChar, isNull, err = b.args[3].EvalString(b.ctx, row); isNull || err != nil {
			return "", isNull, err
		}
	}

	plain, err := replaceMask(val, maskFrom, maskLength, maskChar)
	return plain, false, err
}

func replaceMask(cipher string, maskFrom, maskLength int64, customMaskChar string) (string, error) {
	if maskFrom < 1 {
		// 遵循sql字符串约定，索引参数从1开始
		return "", fmt.Errorf("'maskFrom' (%d) out of range [1, +inf]", maskFrom)
	}

	if maskLength < 1 || maskLength > math.MaxInt8 {
		return "", fmt.Errorf("'maskLength' (%d) out of range [1, %d]", maskLength, math.MaxInt8)
	}

	plain, err := udf.GetClient().Decrypt(cipher)
	if err != nil {
		return "", err
	}

	length := int64(len(plain))
	if length < maskFrom {
		// maskFrom 大于 字符串长度时，忽略替换操作
		return plain, nil
	}

	// 转换成从0开始的索引
	start := maskFrom - 1
	end := start + maskLength
	repeat := int(maskLength)

	tokens := []string{}
	if start > 0 {
		tokens = append(tokens, plain[:start])
	}
	if end > length {
		end = length
		repeat = int(end - start)
	}

	if repeat <= len(maskStrs) && len(customMaskChar) == 0 {
		tokens = append(tokens, maskStrs[repeat])
	} else {
		tokens = append(tokens, strings.Repeat(customMaskChar, repeat))
	}
	if end < length {
		tokens = append(tokens, plain[end:])
	}
	return strings.Join(tokens, ""), nil
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

type mctechGetFullSQLFunctionClass struct {
	baseFunctionClass
}

func (c *mctechGetFullSQLFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	var (
		argTps []types.EvalType
		sig    builtinFunc
	)

	length := len(args)
	switch length {
	case 2:
		argTps = append(argTps, types.ETDatetime, types.ETInt)
	case 3:
		argTps = append(argTps, types.ETDatetime, types.ETInt, types.ETString)
	default:
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs("mc_get_full_sql")
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxFieldCharLength)
	sig = &builtinMCTechGetFullSQLSig{bf}
	return sig, nil
}

type builtinMCTechGetFullSQLSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechGetFullSQLSig) Clone() builtinFunc {
	newSig := &builtinMCTechGetFullSQLSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMCTechGetFullSQLSig) evalString(row chunk.Row) (sql string, isNull bool, err error) {
	var (
		at   types.Time
		txID int64
	)

	if at, isNull, err = b.args[0].EvalTime(b.ctx, row); isNull || err != nil {
		return "", isNull, err
	}

	if txID, isNull, err = b.args[1].EvalInt(b.ctx, row); isNull || err != nil {
		return "", isNull, err
	}

	var group = ""
	if len(b.args) == 3 {
		val, isNull, err := b.args[2].EvalString(b.ctx, row)
		if err != nil {
			return "", isNull, err
		}
		if !isNull {
			group = val
		}
	}

	fullsql, isNull, err := udf.GetFullSQL(at, txID, group)
	if err != nil {
		return "", true, err
	}
	return fullsql, isNull, nil
}

// --------------------------------------------------------------

type mctechDataWarehouseIndexInfoFunctionClass struct {
	baseFunctionClass
}

func (c *mctechDataWarehouseIndexInfoFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson)
	if err != nil {
		return nil, err
	}
	sig := &builtinMCTechDataWarehouseIndexInfoSig{bf}
	return sig, nil
}

type builtinMCTechDataWarehouseIndexInfoSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechDataWarehouseIndexInfoSig) Clone() builtinFunc {
	newSig := &builtinMCTechDataWarehouseIndexInfoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (p *builtinMCTechDataWarehouseIndexInfoSig) evalJSON(row chunk.Row) (types.BinaryJSON, bool, error) {
	mctx, err := mctech.GetContext(p.ctx)
	if err != nil {
		return types.CreateBinaryJSON(nil), true, err
	}
	info, err := mctx.GetDWIndexInfo()
	if err != nil {
		return types.CreateBinaryJSON(nil), true, err
	}
	return types.CreateBinaryJSON(info.ToMap()), false, nil
}

// --------------------------------------------------------------

type mctechHelpFunctionClass struct {
	baseFunctionClass
}

func (c *mctechHelpFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxFieldCharLength)
	sig := &builtinMCTechHelpSig{bf}
	return sig, nil
}

type builtinMCTechHelpSig struct {
	baseBuiltinFunc
}

func (b *builtinMCTechHelpSig) Clone() builtinFunc {
	newSig := &builtinMCTechHelpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMCTechHelpSig) evalString(row chunk.Row) (string, bool, error) {
	lst := []string{}
	for i, item := range mctechFunctionHelps {
		if item.hidden {
			continue
		}

		if i > 0 {
			lst = append(lst, "============================ function split =======================================")
		}
		lst = append(lst, fmt.Sprintf("FUNCTION:: %s|%s", item.name, item.shortName))
		if len(item.signatures) > 0 {
			for j, sign := range item.signatures {
				if j > 0 {
					lst = append(lst, "  ------------------------------ overload split ----------------------------------------")
				}
				parameters := []string{}
				for _, p := range sign.parameters {
					parameters = append(parameters, fmt.Sprintf("%s %s", p.name, p.tp))
				}
				lst = append(lst, fmt.Sprintf("  SIGNATURE:: (%s) %s", strings.Join(parameters, ", "), sign.returnType.tp))
				lst = append(lst, fmt.Sprintf("    %s", sign.description))
				if len(parameters) > 0 {
					lst = append(lst, "  PARAMETERS::")
					for _, p := range sign.parameters {
						lst = append(lst, fmt.Sprintf("    %s: %s", p.name, p.description))
					}
				}
				if sign.returnType.description != "" && sign.returnType.tp != "" {
					lst = append(lst, "  RETURNS::")
					lst = append(lst, fmt.Sprintf("    %s", sign.returnType.description))
				}
			}
		}
	}

	return strings.Join(lst, "\n"), false, nil
}

// --------------------------------------------------------------

// IsValidMCTechSequenceExpr returns true if exprNode is a valid MCTechSequence expression.
// Here `valid` means it is consistent with the given fieldType's decimal.
func IsValidMCTechSequenceExpr(exprNode ast.ExprNode, _ *types.FieldType) bool {
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
func GetBigIntValue(ctx sessionctx.Context, v interface{}, _ byte, _ int) (d types.Datum, err error) {
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
