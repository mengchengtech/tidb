package mctech

import (
	"path/filepath"
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

func initLogger() {
	if fullSqlLogger != nil {
		return
	}

	globalConfig := config.GetGlobalConfig()
	sqlTraceConfig := globalConfig.MCTech.SqlTrace
	cfg := globalConfig.Log.ToLogConfig()
	// copy the global log config to full sql log config
	fsConfig := cfg.Config
	fsConfig.Level = "info"
	fsConfig.Format = "json"
	fsConfig.DisableTimestamp = true
	fsConfig.DisableStacktrace = true
	fsConfig.DisableCaller = true
	fsConfig.File.MaxDays = sqlTraceConfig.FileMaxDays // default 7 days
	fsConfig.File.MaxSize = sqlTraceConfig.FileMaxSize // 300MB

	if sqlTraceConfig.Filename != "" {
		fsConfig.File.Filename = sqlTraceConfig.Filename
	} else {
		logDir := filepath.Dir(globalConfig.Log.File.Filename)
		fsConfig.File.Filename = filepath.Join(logDir, "mctech_tidb_full_sql.log")
	}
	logger, _, err := log.InitLogger(&fsConfig)
	if err != nil {
		panic(errors.Trace(err))
	}

	fullSqlLogger = logger
}

type LobTimeObject struct {
	Query   time.Duration
	Parse   time.Duration
	Compile time.Duration
	Cop     time.Duration
	Ready   time.Duration
	Render  time.Duration
}

func (lt *LobTimeObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("Query", lt.Query)
	enc.AddDuration("Parse", lt.Parse)
	enc.AddDuration("Compile", lt.Compile)
	enc.AddDuration("Cop", lt.Cop)
	enc.AddDuration("Ready", lt.Ready)
	enc.AddDuration("Render", lt.Render)
	return nil
}
