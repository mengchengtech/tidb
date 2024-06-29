package interceptor

import (
	"time"

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

	tikv    tikvTimeObject
	tiflash time.Duration // 从执行计划中汇总统计的TiFlash执行Coprocessor 耗时
	tx      *txTimeObject // 提交事务相关的信息（含显示事务/隐式事务）
}

type tikvTimeObject struct {
	cop     time.Duration // 直接从ExecDetails.CopTime获取到的时间。TiDB Coprocessor 算子等待所有任务在 TiKV 上并行执行完毕耗费的自然时间。如果存在并行任务的话，这个时间一般小于各个并行任务的总时间
	process time.Duration // 从ExecDetails.TimeDetail.ProcessTime获取到的tikv处理请求的过程总共用时。大多数时候都可以替代表示用于CPU的时间
	// TODO 通过两种方式获取用于TiKV的时间，作个对比
	process2 time.Duration // 从执行计划中汇总统计的TiKV执行Coprocessor 耗时
}

func (t *tikvTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("cop", t.cop)
	enc.AddDuration("process", t.process)
	enc.AddDuration("process2", t.process2)
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
	enc.AddObject("tikv", &lt.tikv)
	enc.AddDuration("tiflash", lt.tiflash)
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
