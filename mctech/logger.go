package mctech

import (
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var fullSqlLogger *zap.Logger

func F() *zap.Logger {
	if fullSqlLogger != nil {
		return fullSqlLogger
	}

	// 只能懒加载，需要在启动时先加载 config模块
	once := &sync.Once{}
	once.Do(initLogger)
	return fullSqlLogger
}

func newZapEncoder(cfg *log.Config) zapcore.Encoder {
	cc := zapcore.EncoderConfig{
		TimeKey:        "", // 不记录生成日志时的time
		LevelKey:       "", // 不记录日志级别
		NameKey:        "", // 不记录日志对象的名称
		CallerKey:      "", // 不记录日志所在方法调用者信息
		MessageKey:     "", // 不记录日志的"message"内容
		StacktraceKey:  "", // 不记录日志调用时的调用堆栈信息
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     log.DefaultTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   log.ShortCallerEncoder,
	}
	return zapcore.NewJSONEncoder(cc)
}

func initLogger() {
	if fullSqlLogger != nil {
		return
	}

	globalConfig := config.GetGlobalConfig()
	sqlTraceConfig := globalConfig.MCTech.Metrics.SqlTrace
	cfg := globalConfig.Log.ToLogConfig()
	// copy the global log config to full sql log config
	fsConfig := cfg.Config
	fsConfig.Level = ""
	fsConfig.Format = "json"
	fsConfig.DisableTimestamp = true
	fsConfig.DisableStacktrace = true
	fsConfig.DisableCaller = true
	fsConfig.File.MaxDays = sqlTraceConfig.FileMaxDays // default 7 days
	fsConfig.File.MaxSize = sqlTraceConfig.FileMaxSize // 1024MB
	fsConfig.File.Filename = sqlTraceConfig.Filename

	logger, prop, err := log.InitLogger(&fsConfig)
	newEncoder := newZapEncoder(&cfg.Config)
	newCore := log.NewTextCore(newEncoder, prop.Syncer, prop.Level)
	logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return newCore
	}))
	prop.Core = newCore

	if err != nil {
		panic(errors.Trace(err))
	}

	fullSqlLogger = logger
}

type LobTimeObject struct {
	All   time.Duration // 执行总时间
	Parse time.Duration // 解析语法树用时，含mctech扩展
	Plan  time.Duration // 生成执行计划用时
	Cop   time.Duration // cop用时
	Ready time.Duration // 首行准备好用时
	Send  time.Duration // 发送到客户端用时
}

func (lt *LobTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("all", lt.All)
	enc.AddDuration("parse", lt.Parse)
	enc.AddDuration("plan", lt.Plan)
	enc.AddDuration("cop", lt.Cop)
	enc.AddDuration("ready", lt.Ready)
	enc.AddDuration("send", lt.Send)
	return nil
}
