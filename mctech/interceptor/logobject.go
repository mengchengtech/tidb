package interceptor

import (
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap/zapcore"
)

var (
	_ zapcore.ObjectMarshaler = &logSQLTraceObject{}
	_ zapcore.ObjectMarshaler = &logTimeObject{}
	_ zapcore.ObjectMarshaler = &copTimeObject{}
	_ zapcore.ObjectMarshaler = &txTimeObject{}
	_ zapcore.ObjectMarshaler = &logRUStatObject{}
	_ zapcore.ObjectMarshaler = &logMaxCopObject{}
	_ zapcore.ObjectMarshaler = &logTXObject{}
	_ zapcore.ObjectMarshaler = &logWarningObjects{}
	_ zapcore.ObjectMarshaler = &logWarningObject{}
)

// logSQLTraceObject sql trace log object
type logSQLTraceObject struct {
	at       time.Time          // 执行sql开始时间（不含从sql字符串解析成语法树的时间）
	conn     uint64             // SQL 查询客户端连接 ID
	db       string             // 执行sql时的当前库名称
	dbs      string             // 执行的sql中用到的所有数据库名称列表。','分隔
	across   string             // sql中指定的跨库查询的数据库
	client   *clientInfo        // 执行sql的客户端信息
	inTX     bool               // 当前sql是否在事务中
	user     string             // 执行sql时使用的账号
	tenant   string             // 所属租户信息
	txID     uint64             // 事务号(显示事务和隐式事务)
	maxAct   int64              // sql执行过程中读取/生成的最大行数（与rows不一样，中间过程生成的行数多不代表结果集中的行数多）
	info     *sqlStmtInfo       // sql类型
	times    logTimeObject      // 执行过程中各种时间
	maxCop   *logMaxCopObject   // tikv coprocessor相关的信息
	tx       *logTXObject       // 修改数据相关的信息
	ru       logRUStatObject    // 当前sql资源消耗信息
	mem      int64              // 该 SQL 查询执行时占用的最大内存空间
	disk     int64              // 该 SQL 查询执行时占用的最大磁盘空间
	rows     int64              // 查询返回结果行数
	digest   string             // sql 语句的hash
	warnings *logWarningObjects // 执行中生成的警告信息
	sql      string             // 原始sql，或sql片断
	zip      []byte             // 压缩后的sql文本
	err      error              // sql执行错误信息
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (st *logSQLTraceObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("db", st.db)
	enc.AddString("dbs", st.dbs)
	enc.AddString("usr", st.user)
	if len(st.tenant) > 0 {
		enc.AddString("tenant", st.tenant)
	}
	enc.AddString("conn", encode(st.conn))
	if st.client != nil {
		if err := enc.AddObject("client", st.client); err != nil {
			return err
		}
	}
	enc.AddBool("inTX", st.inTX)
	enc.AddString("cat", st.info.category)
	enc.AddString("tp", st.info.sqlType)
	if len(st.across) > 0 {
		enc.AddString("across", st.across)
	}
	enc.AddString("at", st.at.Format(timeFormat))
	enc.AddString("txId", encode(st.txID))
	enc.AddInt64("maxAct", st.maxAct)
	enc.AddString("digest", st.digest)
	enc.AddInt64("rows", st.rows)
	enc.AddInt64("mem", st.mem)
	enc.AddInt64("disk", st.disk)
	if err := enc.AddObject("times", &st.times); err != nil {
		return err
	}
	if err := enc.AddObject("ru", &st.ru); err != nil {
		return err
	}

	if st.maxCop != nil {
		if err := enc.AddObject("maxCop", st.maxCop); err != nil {
			return err
		}
	}
	if st.tx != nil {
		if err := enc.AddObject("tx", st.tx); err != nil {
			return err
		}
	}
	if st.warnings != nil {
		if err := enc.AddObject("warnings", st.warnings); err != nil {
			return err
		}
	}
	if st.err != nil {
		enc.AddString("error", st.err.Error())
	}

	enc.AddString("sql", st.sql)
	if len(st.zip) > 0 {
		enc.AddBinary("zip", st.zip)
	}

	return nil
}

func (st *logSQLTraceObject) getClient() *clientInfo {
	if st.client != nil {
		return st.client
	}
	st.client = &clientInfo{}
	return st.client
}

type clientInfo struct {
	app     string // 执行当前sql的服务名称
	product string // 执行当前sql的服务所属产品线
	pkg     string // 执行当前sql的依赖包
}

func (t *clientInfo) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if len(t.app) > 0 {
		enc.AddString("app", t.app)
		enc.AddString("product", t.product)
	}

	if len(t.pkg) > 0 {
		enc.AddString("pkg", t.pkg)
	}
	return nil
}

type logTimeObject struct {
	all   time.Duration // 执行总时间，执行 SQL 耗费的自然时间
	parse time.Duration // 解析语法树用时，含mctech扩展
	plan  time.Duration // 生成执行计划耗时
	tidb  time.Duration // tidb-server里用时
	send  time.Duration // 发送到客户端用时

	cop copTimeObject // cop task相关的时间
	tx  *txTimeObject // 提交事务相关的信息（含显示事务/隐式事务）
}

type copTimeObject struct {
	wall    time.Duration // tidb 上等待所有的 cop tasks (tikv, tiflash) 执行完毕耗费的自然时间。直接从ExecDetails.CopTime获取到的时间。如果存在并行任务的话，这个时间一般小于各个并行任务的总时间
	tikv    time.Duration // 从ExecDetails.TimeDetail.ProcessTime获取到的tikv处理请求的过程总共用时。执行 SQL 在 TiKV 的处理时间之和，因为数据会并行的发到 TiKV 执行
	tiflash time.Duration // 从执行计划中汇总统计的TiFlash执行Coprocessor 耗时
}

func (t *copTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("wall", t.wall)
	enc.AddDuration("tikv", t.tikv)
	enc.AddDuration("tiflash", t.tiflash)
	return nil
}

type txTimeObject struct {
	prewrite time.Duration // 事务两阶段提交中第一阶段（prewrite 阶段）的耗时
	commit   time.Duration // 事务两阶段提交中第二阶段（commit 阶段）的耗时
}

func (t *txTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("prewrite", t.prewrite)
	enc.AddDuration("commit", t.commit)
	return nil
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (lt *logTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("all", lt.all)
	enc.AddDuration("parse", lt.parse)
	enc.AddDuration("plan", lt.plan)
	enc.AddDuration("tidb", lt.tidb)
	enc.AddDuration("ready", lt.all-lt.send) // 首行结果准备好时间(总执行时间除去发送结果耗时)
	enc.AddDuration("send", lt.send)
	if err := enc.AddObject("cop", &lt.cop); err != nil {
		return err
	}
	if lt.tx != nil {
		if err := enc.AddObject("tx", lt.tx); err != nil {
			return err
		}
	}
	return nil
}

// logRUStatObject ru stats struct whitch is used for trace log.
type logRUStatObject struct {
	rru float64 // sql执行消耗的RRU值
	wru float64 // sql执行消耗的WRU值
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (lr *logRUStatObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddFloat64("rru", lr.rru)
	enc.AddFloat64("wru", lr.wru)
	return nil
}

type logMaxCopObject struct {
	procAddr string        // 用时最长的cop任务所在节点
	procTime time.Duration // 用时最长的cop任务花费的时间
	tasks    int           // Coprocessor 请求数
}

func (lr *logMaxCopObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("procAddr", lr.procAddr)
	enc.AddDuration("procTime", lr.procTime)
	enc.AddInt("tasks", lr.tasks)
	return nil
}

type logTXObject struct {
	keys     int    // 写入 RockDB Key 个数
	size     int    // 写入数据量Bytes
	affected uint64 // sql执行结果影响的数据行数
}

func (lr *logTXObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt("keys", lr.keys)
	enc.AddInt("size", lr.size)
	enc.AddUint64("affected", lr.affected)
	return nil
}

type logWarningObjects struct {
	topN  warningObjects
	total int
}

func (lw *logWarningObjects) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if err := enc.AddArray("topN", lw.topN); err != nil {
		return err
	}
	enc.AddInt("total", lw.total)
	return nil
}

type warningObjects []*logWarningObject

func (w warningObjects) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, o := range w {
		if err := enc.AppendObject(o); err != nil {
			return err
		}
	}
	return nil
}

type logWarningObject struct {
	msg   string
	extra bool
}

func (lw *logWarningObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("msg", lw.msg)
	enc.AddBool("extra", lw.extra)
	return nil
}

func newWarnings(rawList []variable.JSONSQLWarnForSlowLog) *logWarningObjects {
	length := len(rawList)
	if length == 0 {
		return nil
	}

	lst := make([]*logWarningObject, 0, length)
	for _, log := range rawList {
		lst = append(lst, &logWarningObject{
			msg:   log.Message,
			extra: log.IsExtra,
		})
	}

	if length > 10 {
		lst = lst[0:10]
	}
	return &logWarningObjects{
		topN:  lst,
		total: length,
	}
}

type sqlStmtInfo struct {
	category string // sql语句分类
	sqlType  string // sql语句类型
	modified bool   // sql语句是否修改数据
}

var (
	sqlPrepareInfo    = &sqlStmtInfo{"exec", "prepare", false}
	sqlDeallocateInfo = &sqlStmtInfo{"exec", "deallocate", false}
	sqlBeginInfo      = &sqlStmtInfo{"tx", "begin", false}
	sqlRollbackInfo   = &sqlStmtInfo{"tx", "rollback", true}
	sqlCommitInfo     = &sqlStmtInfo{"tx", "commit", true}
	sqlDeleteInfo     = &sqlStmtInfo{"dml", "delete", true}
	sqlUpdateInfo     = &sqlStmtInfo{"dml", "update", true}
	sqlInsertInfo     = &sqlStmtInfo{"dml", "insert", true}
	sqlSelectInfo     = &sqlStmtInfo{"dml", "select", false}
	sqlLoadInfo       = &sqlStmtInfo{"batch", "load", true}
	sqlTruncateInfo   = &sqlStmtInfo{"batch", "truncate", true}
	sqlSetInfo        = &sqlStmtInfo{"misc", "set", false}
	sqlLockInfo       = &sqlStmtInfo{"misc", "lock", false}
	sqlUnlockInfo     = &sqlStmtInfo{"misc", "unlock", false}
	// sqlUseInfo        = &sqlStmtInfo{"misc", "use", false}
	sqlCallInfo = &sqlStmtInfo{"misc", "call", false}
	sqlDoInfo   = &sqlStmtInfo{"misc", "do", false}
)

const chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func encode(num uint64) string {
	bytes := []byte{}
	for num > 0 {
		bytes = append(bytes, chars[num%62])
		num = num / 62
	}

	for left, right := 0, len(bytes)-1; left < right; left, right = left+1, right-1 {
		bytes[left], bytes[right] = bytes[right], bytes[left]
	}

	return string(bytes)
}
