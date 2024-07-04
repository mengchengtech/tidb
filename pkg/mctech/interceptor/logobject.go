package interceptor

import (
	"time"

	"go.uber.org/zap/zapcore"
)

var (
	_ zapcore.ObjectMarshaler = &logTimeObject{}
	_ zapcore.ObjectMarshaler = &logRUStatObject{}
)

// logTimeObject time data struct whitch is used for trace log.
type logTimeObject struct {
	all   time.Duration // 执行总时间
	parse time.Duration // 解析语法树用时，含mctech扩展
	plan  time.Duration // 生成执行计划用时
	tidb  time.Duration // 除cop任务外用时（一般发生在tidb节点）
	cop   time.Duration // cop用时（一般发生在tikv和tiflash节点）
	copTK time.Duration // tikv cop用时
	copTF time.Duration // tiflash cop用时
	ready time.Duration // 首行准备好用时
	send  time.Duration // 发送到客户端用时
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (lt *logTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("all", lt.all)
	enc.AddDuration("parse", lt.parse)
	enc.AddDuration("plan", lt.plan)
	enc.AddDuration("tidb", lt.tidb)
	enc.AddDuration("cop", lt.cop)
	enc.AddDuration("copTK", lt.copTK)
	enc.AddDuration("copTF", lt.copTF)
	enc.AddDuration("ready", lt.ready)
	enc.AddDuration("send", lt.send)
	return nil
}

// logRUStatObject ru stats struct whitch is used for trace log.
type logRUStatObject struct {
	RRU float64
	WRU float64
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (lr *logRUStatObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddFloat64("rru", lr.RRU)
	enc.AddFloat64("wru", lr.WRU)
	return nil
}
