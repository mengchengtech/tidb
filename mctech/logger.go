package mctech

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var (
	fullSqlLogger,
	largeSqlLogger *zap.Logger
)

func F() *zap.Logger {
	if fullSqlLogger != nil {
		return fullSqlLogger
	}

	// 只能懒加载，需要在启动时先加载 config模块
	once := &sync.Once{}
	once.Do(initFullSqlLogger)
	return fullSqlLogger
}

func L() *zap.Logger {
	if largeSqlLogger != nil {
		return largeSqlLogger
	}

	// 只能懒加载，需要在启动时先加载 config模块
	once := &sync.Once{}
	once.Do(initLargeSqlLogger)
	return largeSqlLogger
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

var _pool = buffer.NewPool()

type largeLogEncoder struct{}

func (e *largeLogEncoder) EncodeEntry(entry zapcore.Entry, _ []zapcore.Field) (*buffer.Buffer, error) {
	b := _pool.Get()
	fmt.Fprintf(b, "# TIME: %s\n", entry.Time.Format(time.RFC3339Nano))
	fmt.Fprintf(b, "%s\n", entry.Message)
	return b, nil
}

func (e *largeLogEncoder) Clone() zapcore.Encoder                          { return e }
func (e *largeLogEncoder) AddArray(string, zapcore.ArrayMarshaler) error   { return nil }
func (e *largeLogEncoder) AddObject(string, zapcore.ObjectMarshaler) error { return nil }
func (e *largeLogEncoder) AddBinary(string, []byte)                        {}
func (e *largeLogEncoder) AddByteString(string, []byte)                    {}
func (e *largeLogEncoder) AddBool(string, bool)                            {}
func (e *largeLogEncoder) AddComplex128(string, complex128)                {}
func (e *largeLogEncoder) AddComplex64(string, complex64)                  {}
func (e *largeLogEncoder) AddDuration(string, time.Duration)               {}
func (e *largeLogEncoder) AddFloat64(string, float64)                      {}
func (e *largeLogEncoder) AddFloat32(string, float32)                      {}
func (e *largeLogEncoder) AddInt(string, int)                              {}
func (e *largeLogEncoder) AddInt64(string, int64)                          {}
func (e *largeLogEncoder) AddInt32(string, int32)                          {}
func (e *largeLogEncoder) AddInt16(string, int16)                          {}
func (e *largeLogEncoder) AddInt8(string, int8)                            {}
func (e *largeLogEncoder) AddString(string, string)                        {}
func (e *largeLogEncoder) AddTime(string, time.Time)                       {}
func (e *largeLogEncoder) AddUint(string, uint)                            {}
func (e *largeLogEncoder) AddUint64(string, uint64)                        {}
func (e *largeLogEncoder) AddUint32(string, uint32)                        {}
func (e *largeLogEncoder) AddUint16(string, uint16)                        {}
func (e *largeLogEncoder) AddUint8(string, uint8)                          {}
func (e *largeLogEncoder) AddUintptr(string, uintptr)                      {}
func (e *largeLogEncoder) AddReflected(string, interface{}) error          { return nil }
func (e *largeLogEncoder) OpenNamespace(string)                            {}

func initLargeSqlLogger() {
	if largeSqlLogger != nil {
		return
	}

	globalConfig := config.GetGlobalConfig()
	largeLogConfig := globalConfig.MCTech.Metrics.LargeLog
	cfg := globalConfig.Log.ToLogConfig()

	sqConfig := cfg.Config
	sqConfig.Level = ""
	sqConfig.File.MaxDays = largeLogConfig.FileMaxDays // default 1 days
	sqConfig.File.MaxSize = largeLogConfig.FileMaxSize // 1024MB
	sqConfig.File.Filename = largeLogConfig.Filename

	// create the large log logger
	logger, prop, err := log.InitLogger(&sqConfig)
	if err != nil {
		panic(errors.Trace(err))
	}

	newCore := log.NewTextCore(&largeLogEncoder{}, prop.Syncer, prop.Level)
	logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return newCore
	}))
	prop.Core = newCore
	largeSqlLogger = logger
}

func initFullSqlLogger() {
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
	fsConfig.File.MaxDays = sqlTraceConfig.FileMaxDays // default 1 days
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
