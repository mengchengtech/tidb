package interceptor

import (
	"time"

	"go.uber.org/zap/zapcore"
)

var (
	_ zapcore.ObjectMarshaler = &logTimeObject{}
)

// logTimeObject time data struct whitch is used for trace log.
type logTimeObject struct {
	all   time.Duration // 执行总时间
	parse time.Duration // 解析语法树用时，含mctech扩展
	plan  time.Duration // 生成执行计划用时
	cop   time.Duration // cop用时
	ready time.Duration // 首行准备好用时
	send  time.Duration // 发送到客户端用时
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (lt *logTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("all", lt.all)
	enc.AddDuration("parse", lt.parse)
	enc.AddDuration("plan", lt.plan)
	enc.AddDuration("cop", lt.cop)
	enc.AddDuration("ready", lt.ready)
	enc.AddDuration("send", lt.send)
	return nil
}
