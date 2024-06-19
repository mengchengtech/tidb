package interceptor

import (
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap/zapcore"
)

var (
	_ zapcore.ObjectMarshaler = &logTimeObject{}
	_ zapcore.ObjectMarshaler = &logRUStatObject{}
)

type logTimeObject struct {
	all   time.Duration // 执行总时间，执行 SQL 耗费的自然时间
	parse time.Duration // 解析语法树用时，含mctech扩展
	plan  time.Duration // 生成执行计划耗时
	tidb  time.Duration // tidb-server里用时
	ready time.Duration // 首行结果准备好时间(总执行时间除去发送结果耗时)
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
	enc.AddDuration("ready", lt.ready)
	enc.AddDuration("send", lt.send)
	enc.AddObject("cop", &lt.cop)
	if lt.tx != nil {
		enc.AddObject("tx", lt.tx)
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
	enc.AddArray("topN", lw.topN)
	enc.AddInt("total", lw.total)
	return nil
}

type warningObjects []*logWarningObject

func (w warningObjects) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, o := range w {
		enc.AppendObject(o)
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
