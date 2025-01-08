package udf

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const dateFormat = "2006-01-02"

// GetFullSQL get full sql from disk compact file
func GetFullSQL(at types.Time, txID int64, group string) (sql string, isNull bool, err error) {
	config := config.GetMCTechConfig()
	fullSQLDir := config.Metrics.SQLTrace.FullSQLDir
	if fullSQLDir == "" {
		return "", true, errors.New("未设置 mctech.metrics.sql-trace.full-sql-dir 配置项")
	}

	if group == "" {
		group = config.Metrics.SQLTrace.Group
	}

	if group != "" {
		fullSQLDir = path.Join(fullSQLDir, group)
	}

	var gotime time.Time
	if gotime, err = at.GoTime(time.Local); err != nil {
		return "", true, err
	}
	date := gotime.Format(dateFormat)
	hour := gotime.Hour()
	fileDir := path.Join(fullSQLDir, date, strconv.Itoa(hour))
	fullPath := path.Join(fileDir, fmt.Sprintf("%d-%d.gz", gotime.UnixMilli(), txID))
	if _, err := os.Stat(fullPath); err != nil {
		if pe, ok := err.(*os.PathError); ok {
			if pe.Err == syscall.ENOENT {
				return "", true, nil
			}
		}

		logutil.BgLogger().Error("get compress sql file error", zap.Time("at", gotime), zap.Int64("txID", txID), zap.Error(err))
		return "", true, fmt.Errorf("get compress sql file error, [%s, %d, %s]", at, txID, group)
	}

	var fi *os.File
	if fi, err = os.Open(filepath.Clean(fullPath)); err != nil {
		logutil.BgLogger().Error("load full sql error", zap.Time("at", gotime), zap.Int64("txID", txID), zap.Error(err))
		return "", true, fmt.Errorf("load full sql error, [%s, %d, %s]", at, txID, group)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			logutil.BgLogger().Warn("[fi.Close] received error", zap.Error(err))
		}
	}()

	var gz *gzip.Reader
	if gz, err = gzip.NewReader(fi); err != nil {
		return "", true, err
	}
	defer func() {
		if err := gz.Close(); err != nil {
			logutil.BgLogger().Warn("[gz.Close] received error", zap.Error(err))
		}
	}()

	var b []byte
	if b, err = io.ReadAll(gz); err != nil {
		return "", true, err
	}
	sql = string(b)
	return sql, false, nil
}
