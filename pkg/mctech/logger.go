package mctech

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var (
	fullQueryLogger  *zap.Logger
	largeQueryLogger *zap.Logger
	fqOnce           = &sync.Once{}
	lqOnce           = &sync.Once{}
)

// F Get full sql trace logger
func F() *zap.Logger {
	// 只能懒加载，需要在启动时先加载 config模块
	fqOnce.Do(func() {
		fullQueryLogger = initFullQueryLogger()
	})
	return fullQueryLogger
}

// L Get large query logger
func L() *zap.Logger {
	// 只能懒加载，需要在启动时先加载 config模块
	lqOnce.Do(func() {
		largeQueryLogger = initLargeQueryLogger()
	})
	return largeQueryLogger
}

func newZapEncoder(_ *log.Config) zapcore.Encoder {
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

type largeQueryEncoder struct{}

func (e *largeQueryEncoder) EncodeEntry(entry zapcore.Entry, _ []zapcore.Field) (*buffer.Buffer, error) {
	b := _pool.Get()
	fmt.Fprintf(b, "# TIME: %s\n", entry.Time.Format(time.RFC3339Nano))
	fmt.Fprintf(b, "%s\n", entry.Message)
	return b, nil
}

func (e *largeQueryEncoder) Clone() zapcore.Encoder                          { return e }
func (e *largeQueryEncoder) AddArray(string, zapcore.ArrayMarshaler) error   { return nil }
func (e *largeQueryEncoder) AddObject(string, zapcore.ObjectMarshaler) error { return nil }
func (e *largeQueryEncoder) AddBinary(string, []byte)                        {}
func (e *largeQueryEncoder) AddByteString(string, []byte)                    {}
func (e *largeQueryEncoder) AddBool(string, bool)                            {}
func (e *largeQueryEncoder) AddComplex128(string, complex128)                {}
func (e *largeQueryEncoder) AddComplex64(string, complex64)                  {}
func (e *largeQueryEncoder) AddDuration(string, time.Duration)               {}
func (e *largeQueryEncoder) AddFloat64(string, float64)                      {}
func (e *largeQueryEncoder) AddFloat32(string, float32)                      {}
func (e *largeQueryEncoder) AddInt(string, int)                              {}
func (e *largeQueryEncoder) AddInt64(string, int64)                          {}
func (e *largeQueryEncoder) AddInt32(string, int32)                          {}
func (e *largeQueryEncoder) AddInt16(string, int16)                          {}
func (e *largeQueryEncoder) AddInt8(string, int8)                            {}
func (e *largeQueryEncoder) AddString(string, string)                        {}
func (e *largeQueryEncoder) AddTime(string, time.Time)                       {}
func (e *largeQueryEncoder) AddUint(string, uint)                            {}
func (e *largeQueryEncoder) AddUint64(string, uint64)                        {}
func (e *largeQueryEncoder) AddUint32(string, uint32)                        {}
func (e *largeQueryEncoder) AddUint16(string, uint16)                        {}
func (e *largeQueryEncoder) AddUint8(string, uint8)                          {}
func (e *largeQueryEncoder) AddUintptr(string, uintptr)                      {}
func (e *largeQueryEncoder) AddReflected(string, any) error                  { return nil }
func (e *largeQueryEncoder) OpenNamespace(string)                            {}

var pattern = regexp.MustCompile("(?i){([^}]+)}")

func getHostName() string {
	failpoint.Inject("GetHostName", func() {
		failpoint.Return("tidb01")
	})

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

func getRealLogFile(filename string) (ret string, err error) {
	matches := pattern.FindStringSubmatch(filename)
	if matches == nil {
		return filename, nil
	}

	hostname := getHostName()
	defer func() {
		if r := recover(); r != nil {
			err, _ = r.(error)
		}
	}()
	realFileName := pattern.ReplaceAllStringFunc(filename, func(sub string) string {
		switch strings.ToLower(sub) {
		case "{hostname}":
			return hostname
		default:
			errMsg := fmt.Sprintf("metrics log filename template DO NOT support '%s' only allow '%s'。", matches[1], "hostname")
			logutil.BgLogger().Error(errMsg)
			panic(errors.New(errMsg))
		}
	})

	return realFileName, nil
}

func initLargeQueryLogger() *zap.Logger {
	globalConfig := config.GetGlobalConfig()
	largeQueryConfig := globalConfig.MCTech.Metrics.LargeQuery
	cfg := globalConfig.Log.ToLogConfig()

	sqConfig := cfg.Config
	sqConfig.Level = ""
	sqConfig.File.MaxDays = largeQueryConfig.FileMaxDays
	sqConfig.File.MaxSize = largeQueryConfig.FileMaxSize
	var err error
	if sqConfig.File.Filename, err = getRealLogFile(largeQueryConfig.Filename); err != nil {
		panic(err)
	}

	// create the large query logger
	logger, prop, err := log.InitLogger(&sqConfig)
	if err != nil {
		panic(errors.Trace(err))
	}

	newCore := log.NewTextCore(&largeQueryEncoder{}, prop.Syncer, prop.Level)
	logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return newCore
	}))
	prop.Core = newCore
	return logger
}

func initFullQueryLogger() *zap.Logger {
	globalConfig := config.GetGlobalConfig()
	sqlTraceConfig := globalConfig.MCTech.Metrics.SQLTrace
	cfg := globalConfig.Log.ToLogConfig()
	// copy the global log config to full sql log config
	fsConfig := cfg.Config
	fsConfig.Level = ""
	fsConfig.Format = "json"
	fsConfig.DisableTimestamp = true
	fsConfig.DisableStacktrace = true
	fsConfig.DisableCaller = true
	fsConfig.File.MaxDays = sqlTraceConfig.FileMaxDays
	fsConfig.File.MaxSize = sqlTraceConfig.FileMaxSize
	var err error
	if fsConfig.File.Filename, err = getRealLogFile(sqlTraceConfig.Filename); err != nil {
		panic(err)
	}

	logger, prop, err := log.InitLogger(&fsConfig)
	if err != nil {
		panic(errors.Trace(err))
	}

	newEncoder := newZapEncoder(&cfg.Config)
	newCore := log.NewTextCore(newEncoder, prop.Syncer, prop.Level)
	logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return newCore
	}))
	prop.Core = newCore

	return logger
}

// LogTimeObject time data struct whitch is used for trace log.
type LogTimeObject struct {
	All   time.Duration // 执行总时间
	Parse time.Duration // 解析语法树用时，含mctech扩展
	Plan  time.Duration // 生成执行计划用时
	Cop   time.Duration // cop用时
	Ready time.Duration // 首行准备好用时
	Send  time.Duration // 发送到客户端用时
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface.
func (lt *LogTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("all", lt.All)
	enc.AddDuration("parse", lt.Parse)
	enc.AddDuration("plan", lt.Plan)
	enc.AddDuration("cop", lt.Cop)
	enc.AddDuration("ready", lt.Ready)
	enc.AddDuration("send", lt.Send)
	return nil
}
