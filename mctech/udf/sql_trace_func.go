package udf

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
)

func GetFullSql(node, conn string, at types.Time) (sql string, err error) {
	config := config.GetMCTechConfig()
	fullSqlDir := config.Metrics.SQLTrace.FullSqlDir
	if fullSqlDir == "" {
		return "", errors.New("未设置 mctech_metrics_sql_trace_full_sql_dir 全局变量的值")
	}
	date, err := at.DateFormat("%Y-%m-%d")
	if err != nil {
		return "", err
	}

	gotime, err := at.GoTime(time.Local)
	if err != nil {
		return "", err
	}
	fullPath := path.Join(fullSqlDir, date, node, conn, fmt.Sprintf("%d.gz", gotime.UnixMilli()))
	if _, err := os.Stat(fullPath); err != nil {
		return "", err
	}

	var fi *os.File
	if fi, err = os.Open(fullPath); err != nil {
		return "", err
	}
	defer fi.Close()
	var gz *gzip.Reader
	if gz, err = gzip.NewReader(fi); err != nil {
		return "", err
	}
	defer gz.Close()
	if b, err := io.ReadAll(gz); err != nil {
		return "", err
	} else {
		sql = string(b)
	}
	return sql, nil
}
