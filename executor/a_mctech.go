// add by zhangbing

package executor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

func init() {
	// mctech functions
	funcName2Alias["mctech_sequence"] = ast.MCTechSequence
	funcName2Alias["mctech_version_just_pass"] = ast.MCTechVersionJustPass
	funcName2Alias["mctech_decrypt"] = ast.MCTechDecrypt
	funcName2Alias["mctech_encrypt"] = ast.MCTechEncrypt
	// add end
}

// buildMCTech builds a explain executor. `e.rows` collects final result to `MCTechExec`.
func (b *executorBuilder) buildMCTech(v *plannercore.MCTech) Executor {
	explainExec := &MCTechExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		mctech:       v,
	}
	return explainExec
}

// MCTechExec represents an explain executor.
type MCTechExec struct {
	baseExecutor

	// 对应的执行计划类的实例
	mctech *plannercore.MCTech
	// 需要返回的行列数据
	rows   [][]*types.Datum
	cursor int
}

// Open implements the Executor Open interface.
func (e *MCTechExec) Open(ctx context.Context) error {
	// MCTECH语句不需要做额外的事情
	return nil
}

// Close implements the Executor Close interface.
func (e *MCTechExec) Close() error {
	// MCTECH语句不需要做额外的事情，只需要简单的把返回结果清除下
	e.rows = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *MCTechExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		if err := e.mctech.RenderResult(ctx); err != nil {
			return err
		}
		e.rows = e.mctech.Rows
	}

	// 一定要执行这行代码，初始化返回结果集存储空间，否则程序会卡死
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := mathutil.Min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		// 根据schema里定义的类型调用不同的方法
		for j := range e.rows[i] {
			datum := e.rows[i][j]
			// log.Info(fmt.Sprintf("%d -> kind: %d", j, datum.Kind()))
			req.AppendDatum(j, datum)
		}
	}
	e.cursor += numCurRows
	return nil
}

func (e *PrepareExec) beforePrepare(ctx context.Context) error {
	if config.GetMCTechConfig().Tenant.ForbiddenPrepare {
		return errors.New("[mctech] PREPARE not allowed")
	}
	return nil
}

func (e *PrepareExec) afterParseSql(ctx context.Context, stmts []ast.StmtNode) (err error) {
	handler := mctech.GetHandler()
	var mctx mctech.Context
	mctx, err = mctech.GetContext(ctx)
	if err != nil {
		return err
	}

	if mctx != nil {
		modifyCtx := mctx.(mctech.BaseContextAware).BaseContext().(mctech.ModifyContext)
		modifyCtx.SetUsingTenantParam(true)
		if _, err = handler.ApplyAndCheck(mctx, stmts); err != nil {
			if strFmt, ok := e.ctx.(fmt.Stringer); ok {
				logutil.Logger(ctx).Warn("mctech SQL failed", zap.Error(err), zap.Stringer("session", strFmt), zap.String("SQL", e.sqlText))
			}
			return err
		}
	}

	return nil
}

// ---------------------------------------------- large sql query -----------------------------------------------

// ParseLargeQueryBatchSize is the batch size of large-query-log lines for a worker to parse, exported for testing.
var ParseLargeQueryBatchSize = 64

// mctechLargeQueryRetriever is used to read large query log data.
type mctechLargeQueryRetriever struct {
	table                 *model.TableInfo
	outputCols            []*model.ColumnInfo
	initialized           bool
	extractor             *plannercore.MCTechLargeQueryExtractor
	files                 []logFile
	fileIdx               int
	fileLine              int
	checker               *mctechLargeQueryChecker
	columnValueFactoryMap map[string]mctechLargeQueryColumnValueFactory
	instanceFactory       func([]types.Datum)

	taskList      chan mctechLargeQueryTask
	stats         *mctechLargeQueryRuntimeStats
	memTracker    *memory.Tracker
	lastFetchSize int64
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func (e *mctechLargeQueryRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if !e.initialized {
		err := e.initialize(ctx, sctx)
		if err != nil {
			return nil, err
		}
		ctx, e.cancel = context.WithCancel(ctx)
		e.initializeAsyncParsing(ctx, sctx)
	}
	return e.dataForLargeQuery(ctx, sctx)
}

func (e *mctechLargeQueryRetriever) initialize(ctx context.Context, sctx sessionctx.Context) error {
	var err error
	var hasProcessPriv bool
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		hasProcessPriv = pm.RequestVerification(sctx.GetSessionVars().ActiveRoles, "", "", "", mysql.ProcessPriv)
	}
	// initialize column value factories.
	e.columnValueFactoryMap = make(map[string]mctechLargeQueryColumnValueFactory, len(e.outputCols))
	for idx, col := range e.outputCols {
		if col.Name.O == util.ClusterTableInstanceColumnName {
			e.instanceFactory, err = getInstanceColumnValueFactory(sctx, idx)
			if err != nil {
				return err
			}
			continue
		}
		factory, err := getLargeQueryColumnValueFactoryByName(sctx, col.Name.O, idx)
		if err != nil {
			return err
		}
		if factory == nil {
			panic(fmt.Sprintf("should never happen, should register new column %v into getLargeQueryColumnValueFactoryByName function", col.Name.O))
		}
		e.columnValueFactoryMap[col.Name.O] = factory
	}
	// initialize checker.
	e.checker = &mctechLargeQueryChecker{
		hasProcessPriv: hasProcessPriv,
		user:           sctx.GetSessionVars().User,
	}
	e.stats = &mctechLargeQueryRuntimeStats{}
	if e.extractor != nil {
		e.checker.enableTimeCheck = e.extractor.Enable
		for _, tr := range e.extractor.TimeRanges {
			startTime := types.NewTime(types.FromGoTime(tr.StartTime), mysql.TypeDatetime, types.MaxFsp)
			endTime := types.NewTime(types.FromGoTime(tr.EndTime), mysql.TypeDatetime, types.MaxFsp)
			timeRange := &timeRange{
				startTime: startTime,
				endTime:   endTime,
			}
			e.checker.timeRanges = append(e.checker.timeRanges, timeRange)
		}
	} else {
		e.extractor = &plannercore.MCTechLargeQueryExtractor{}
	}
	e.initialized = true
	e.files, err = e.getAllFiles(ctx, sctx, config.GetMCTechConfig().Metrics.LargeQuery.Filename)
	if e.extractor.Desc {
		e.reverseLogFiles()
	}
	return err
}

func (e *mctechLargeQueryRetriever) reverseLogFiles() {
	for i := 0; i < len(e.files)/2; i++ {
		j := len(e.files) - i - 1
		e.files[i], e.files[j] = e.files[j], e.files[i]
	}
}

func (e *mctechLargeQueryRetriever) close() error {
	for _, f := range e.files {
		err := f.file.Close()
		if err != nil {
			logutil.BgLogger().Error("close large query log file failed.", zap.Error(err))
		}
	}
	if e.cancel != nil {
		e.cancel()
	}
	e.wg.Wait()
	return nil
}

type parsedLargeQuery struct {
	rows [][]types.Datum
	err  error
}

func (e *mctechLargeQueryRetriever) getNextFile() *os.File {
	if e.fileIdx >= len(e.files) {
		return nil
	}
	file := e.files[e.fileIdx].file
	e.fileIdx++
	if e.stats != nil {
		stat, err := file.Stat()
		if err == nil {
			// ignore the err will be ok.
			e.stats.readFileSize += stat.Size()
			e.stats.readFileNum++
		}
	}
	return file
}

func (e *mctechLargeQueryRetriever) getPreviousFile() *os.File {
	fileIdx := e.fileIdx
	// fileIdx refer to the next file which should be read
	// so we need to set fileIdx to fileIdx - 2 to get the previous file.
	fileIdx = fileIdx - 2
	if fileIdx < 0 {
		return nil
	}
	file := e.files[fileIdx].file
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return nil
	}
	return file
}

func (e *mctechLargeQueryRetriever) parseDataForLargeQuery(ctx context.Context, sctx sessionctx.Context) {
	defer e.wg.Done()
	file := e.getNextFile()
	if file == nil {
		close(e.taskList)
		return
	}
	reader := bufio.NewReader(file)
	e.parseLargeQuery(ctx, sctx, reader, ParseLargeQueryBatchSize)
}

func (e *mctechLargeQueryRetriever) dataForLargeQuery(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	var (
		task mctechLargeQueryTask
		ok   bool
	)
	e.memConsume(-e.lastFetchSize)
	e.lastFetchSize = 0
	for {
		select {
		case task, ok = <-e.taskList:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if !ok {
			return nil, nil
		}
		result := <-task.resultCh
		rows, err := result.rows, result.err
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			continue
		}
		if e.instanceFactory != nil {
			for i := range rows {
				e.instanceFactory(rows[i])
			}
		}
		e.lastFetchSize = calculateDatumsSize(rows)
		return rows, nil
	}
}

type mctechLargeQueryChecker struct {
	// Below fields is used to check privilege.
	hasProcessPriv bool
	user           *auth.UserIdentity
	// Below fields is used to check large query log time valid.
	enableTimeCheck bool
	timeRanges      []*timeRange
}

func (sc *mctechLargeQueryChecker) hasPrivilege(userName string) bool {
	return sc.hasProcessPriv || sc.user == nil || userName == sc.user.Username
}

func (sc *mctechLargeQueryChecker) isTimeValid(t types.Time) bool {
	for _, tr := range sc.timeRanges {
		if sc.enableTimeCheck && (t.Compare(tr.startTime) >= 0 && t.Compare(tr.endTime) <= 0) {
			return true
		}
	}
	return !sc.enableTimeCheck
}

type mctechLargeQueryTask struct {
	resultCh chan parsedLargeQuery
}

type mctechLargeQueryBlock []string

func (e *mctechLargeQueryRetriever) getBatchLog(ctx context.Context, reader *bufio.Reader, offset *offset, num int) ([][]string, error) {
	var line string
	log := make([]string, 0, num)
	var err error
	for i := 0; i < num; i++ {
		for {
			if isCtxDone(ctx) {
				return nil, ctx.Err()
			}
			e.fileLine++
			lineByte, err := getOneLine(reader)
			if err != nil {
				if err == io.EOF {
					e.fileLine = 0
					file := e.getNextFile()
					if file == nil {
						return [][]string{log}, nil
					}
					offset.length = len(log)
					reader.Reset(file)
					continue
				}
				return [][]string{log}, err
			}
			line = string(hack.String(lineByte))
			log = append(log, line)
			if strings.HasSuffix(line, variable.MCTechLargeQuerySQLSuffixStr) {
				if strings.HasPrefix(line, "use") || strings.HasPrefix(line, variable.MCTechLargeQueryRowPrefixStr) {
					continue
				}
				break
			}
		}
	}
	return [][]string{log}, err
}

func (e *mctechLargeQueryRetriever) getBatchLogForReversedScan(ctx context.Context, reader *bufio.Reader, offset *offset, num int) ([][]string, error) {
	// reader maybe change when read previous file.
	inputReader := reader
	defer func() {
		file := e.getNextFile()
		if file != nil {
			inputReader.Reset(file)
		}
	}()
	var line string
	var queries []mctechLargeQueryBlock
	var log []string
	var err error
	hasStartFlag := false
	scanPreviousFile := false
	for {
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		e.fileLine++
		lineByte, err := getOneLine(reader)
		if err != nil {
			if err == io.EOF {
				if len(log) == 0 {
					decomposedLargeQueryTasks := decomposeToLargeQueryTasks(queries, num)
					offset.length = len(decomposedLargeQueryTasks)
					return decomposedLargeQueryTasks, nil
				}
				e.fileLine = 0
				file := e.getPreviousFile()
				if file == nil {
					return decomposeToLargeQueryTasks(queries, num), nil
				}
				reader = bufio.NewReader(file)
				scanPreviousFile = true
				continue
			}
			return nil, err
		}
		line = string(hack.String(lineByte))
		if !hasStartFlag && strings.HasPrefix(line, variable.MCTechLargeQueryStartPrefixStr) {
			hasStartFlag = true
		}
		if hasStartFlag {
			log = append(log, line)
			if strings.HasSuffix(line, variable.MCTechLargeQuerySQLSuffixStr) {
				if strings.HasPrefix(line, "use") || strings.HasPrefix(line, variable.MCTechLargeQueryRowPrefixStr) {
					continue
				}
				queries = append(queries, log)
				if scanPreviousFile {
					break
				}
				log = make([]string, 0, 8)
				hasStartFlag = false
			}
		}
	}
	return decomposeToLargeQueryTasks(queries, num), err
}

func decomposeToLargeQueryTasks(logs []mctechLargeQueryBlock, num int) [][]string {
	if len(logs) == 0 {
		return nil
	}

	//In reversed scan, We should reverse the blocks.
	last := len(logs) - 1
	for i := 0; i < len(logs)/2; i++ {
		logs[i], logs[last-i] = logs[last-i], logs[i]
	}

	decomposedLargeQueryTasks := make([][]string, 0)
	log := make([]string, 0, num*len(logs[0]))
	for i := range logs {
		log = append(log, logs[i]...)
		if i > 0 && i%num == 0 {
			decomposedLargeQueryTasks = append(decomposedLargeQueryTasks, log)
			log = make([]string, 0, len(log))
		}
	}
	if len(log) > 0 {
		decomposedLargeQueryTasks = append(decomposedLargeQueryTasks, log)
	}
	return decomposedLargeQueryTasks
}

func (e *mctechLargeQueryRetriever) parseLargeQuery(ctx context.Context, sctx sessionctx.Context, reader *bufio.Reader, logNum int) {
	defer close(e.taskList)
	offset := offset{offset: 0, length: 0}
	// To limit the num of go routine
	concurrent := sctx.GetSessionVars().Concurrency.DistSQLScanConcurrency()
	ch := make(chan int, concurrent)
	if e.stats != nil {
		e.stats.concurrent = concurrent
	}
	defer close(ch)
	for {
		startTime := time.Now()
		var logs [][]string
		var err error
		if !e.extractor.Desc {
			logs, err = e.getBatchLog(ctx, reader, &offset, logNum)
		} else {
			logs, err = e.getBatchLogForReversedScan(ctx, reader, &offset, logNum)
		}
		if err != nil {
			t := mctechLargeQueryTask{}
			t.resultCh = make(chan parsedLargeQuery, 1)
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			e.sendParsedLargeQueryCh(t, parsedLargeQuery{nil, err})
		}
		if len(logs) == 0 || len(logs[0]) == 0 {
			break
		}
		if e.stats != nil {
			e.stats.readFile += time.Since(startTime)
		}
		failpoint.Inject("mockReadLargeQueryLarge", func(val failpoint.Value) {
			if val.(bool) {
				signals := ctx.Value("signals").([]chan int)
				signals[0] <- 1
				<-signals[1]
			}
		})
		for i := range logs {
			log := logs[i]
			t := mctechLargeQueryTask{}
			t.resultCh = make(chan parsedLargeQuery, 1)
			start := offset
			ch <- 1
			select {
			case <-ctx.Done():
				return
			case e.taskList <- t:
			}
			e.wg.Add(1)
			go func() {
				defer e.wg.Done()
				result, err := e.parseLog(ctx, sctx, log, start)
				e.sendParsedLargeQueryCh(t, parsedLargeQuery{result, err})
				<-ch
			}()
			offset.offset = e.fileLine
			offset.length = 0
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (e *mctechLargeQueryRetriever) sendParsedLargeQueryCh(t mctechLargeQueryTask, re parsedLargeQuery) {
	select {
	case t.resultCh <- re:
	default:
		return
	}
}

func (e *mctechLargeQueryRetriever) parseLog(ctx context.Context, sctx sessionctx.Context, log []string, offset offset) (data [][]types.Datum, err error) {
	start := time.Now()
	logSize := calculateLogSize(log)
	defer e.memConsume(-logSize)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.BgLogger().Warn("large sql query parse large query log panic", zap.Error(err), zap.String("stack", string(buf)))
		}
		if e.stats != nil {
			atomic.AddInt64(&e.stats.parseLog, int64(time.Since(start)))
		}
	}()
	e.memConsume(logSize)
	failpoint.Inject("errorMockParseLargeQueryPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	var row []types.Datum
	user := ""
	tz := sctx.GetSessionVars().Location()
	startFlag := false
	for index, line := range log {
		if isCtxDone(ctx) {
			return nil, ctx.Err()
		}
		fileLine := getLineIndex(offset, index)
		if !startFlag && strings.HasPrefix(line, variable.MCTechLargeQueryStartPrefixStr) {
			row = make([]types.Datum, len(e.outputCols))
			user = ""
			valid := e.setColumnValue(sctx, row, tz, variable.MCTechLargeQueryTimeStr, line[len(variable.MCTechLargeQueryStartPrefixStr):], e.checker, fileLine)
			if valid {
				startFlag = true
			}
			continue
		}
		if startFlag {
			if strings.HasPrefix(line, variable.MCTechLargeQueryRowPrefixStr) {
				line = line[len(variable.MCTechLargeQueryRowPrefixStr):]
				valid := true
				if strings.HasPrefix(line, variable.MCTechLargeQueryUserAndHostStr+variable.MCTechLargeQuerySpaceMarkStr) {
					value := line[len(variable.MCTechLargeQueryUserAndHostStr+variable.MCTechLargeQuerySpaceMarkStr):]
					fields := strings.SplitN(value, "@", 2)
					if len(fields) < 2 {
						continue
					}
					user = parseUserOrHostValue(fields[0])
					if e.checker != nil && !e.checker.hasPrivilege(user) {
						startFlag = false
						continue
					}
					valid = e.setColumnValue(sctx, row, tz, variable.MCTechLargeQueryUserStr, user, e.checker, fileLine)
					if !valid {
						startFlag = false
						continue
					}
					host := parseUserOrHostValue(fields[1])
					valid = e.setColumnValue(sctx, row, tz, variable.MCTechLargeQueryHostStr, host, e.checker, fileLine)
				} else {
					fields, values := splitByColon(line)
					for i := 0; i < len(fields); i++ {
						valid := e.setColumnValue(sctx, row, tz, fields[i], values[i], e.checker, fileLine)
						if !valid {
							startFlag = false
							break
						}
					}
				}
				if !valid {
					startFlag = false
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				if strings.HasPrefix(line, "use") {
					// `use DB` statements in the slow log is used to keep it be compatible with MySQL,
					// since we already get the current DB from the `# DB` field, we can ignore it here,
					// please see https://github.com/pingcap/tidb/issues/17846 for more details.
					continue
				}
				if e.checker != nil && !e.checker.hasPrivilege(user) {
					startFlag = false
					continue
				}
				// Get the sql string, and mark the start flag to false.
				_ = e.setColumnValue(sctx, row, tz, variable.MCTechLargeQuerySQLStr, string(hack.Slice(line)), e.checker, fileLine)
				e.setDefaultValue(row)
				e.memConsume(types.EstimatedMemUsage(row, 1))
				data = append(data, row)
				startFlag = false
			} else {
				startFlag = false
			}
		}
	}
	return data, nil
}

func (e *mctechLargeQueryRetriever) setColumnValue(sctx sessionctx.Context, row []types.Datum, tz *time.Location, field, value string, checker *mctechLargeQueryChecker, lineNum int) bool {
	factory := e.columnValueFactoryMap[field]
	if factory == nil {
		// Fix issue 34320, when slow log time is not in the output columns, the time filter condition is mistakenly discard.
		if field == variable.MCTechLargeQueryTimeStr && checker != nil {
			t, err := ParseTime(value)
			if err != nil {
				err = fmt.Errorf("Parse slow log at line %v, failed field is %v, failed value is %v, error is %v", lineNum, field, value, err)
				sctx.GetSessionVars().StmtCtx.AppendWarning(err)
				return false
			}
			timeValue := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, types.MaxFsp)
			return checker.isTimeValid(timeValue)
		}
		return true
	}
	valid, err := factory(row, value, tz, checker)
	if err != nil {
		err = fmt.Errorf("Parse slow log at line %v, failed field is %v, failed value is %v, error is %v", lineNum, field, value, err)
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return true
	}
	return valid
}

func (e *mctechLargeQueryRetriever) setDefaultValue(row []types.Datum) {
	for i := range row {
		if !row[i].IsNull() {
			continue
		}
		row[i] = table.GetZeroValue(e.outputCols[i])
	}
}

// getAllFiles is used to get all slow-log needed to parse, it is exported for test.
func (e *mctechLargeQueryRetriever) getAllFiles(ctx context.Context, sctx sessionctx.Context, logFilePath string) ([]logFile, error) {
	totalFileNum := 0
	if e.stats != nil {
		startTime := time.Now()
		defer func() {
			e.stats.initialize = time.Since(startTime)
			e.stats.totalFileNum = totalFileNum
		}()
	}
	if e.extractor == nil || !e.extractor.Enable {
		totalFileNum = 1
		file, err := os.Open(logFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, err
		}
		return []logFile{{file: file}}, nil
	}
	var logFiles []logFile
	logDir := filepath.Dir(logFilePath)
	ext := filepath.Ext(logFilePath)
	prefix := logFilePath[:len(logFilePath)-len(ext)]
	handleErr := func(err error) error {
		// Ignore the error and append warning for usability.
		if err != io.EOF {
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}
	files, err := os.ReadDir(logDir)
	if err != nil {
		return nil, err
	}
	walkFn := func(path string, info os.DirEntry) error {
		if info.IsDir() {
			return nil
		}
		// All rotated log files have the same prefix with the original file.
		if !strings.HasPrefix(path, prefix) {
			return nil
		}
		if isCtxDone(ctx) {
			return ctx.Err()
		}
		totalFileNum++
		file, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return handleErr(err)
		}
		skip := false
		defer func() {
			if !skip {
				terror.Log(file.Close())
			}
		}()
		// Get the file start time.
		fileStartTime, err := e.getFileStartTime(ctx, file)
		if err != nil {
			return handleErr(err)
		}
		start := types.NewTime(types.FromGoTime(fileStartTime), mysql.TypeDatetime, types.MaxFsp)
		notInAllTimeRanges := true
		for _, tr := range e.checker.timeRanges {
			if start.Compare(tr.endTime) <= 0 {
				notInAllTimeRanges = false
				break
			}
		}
		if notInAllTimeRanges {
			return nil
		}

		// Get the file end time.
		fileEndTime, err := e.getFileEndTime(ctx, file)
		if err != nil {
			return handleErr(err)
		}
		end := types.NewTime(types.FromGoTime(fileEndTime), mysql.TypeDatetime, types.MaxFsp)
		inTimeRanges := false
		for _, tr := range e.checker.timeRanges {
			if !(start.Compare(tr.endTime) > 0 || end.Compare(tr.startTime) < 0) {
				inTimeRanges = true
				break
			}
		}
		if !inTimeRanges {
			return nil
		}
		_, err = file.Seek(0, io.SeekStart)
		if err != nil {
			return handleErr(err)
		}
		logFiles = append(logFiles, logFile{
			file:  file,
			start: fileStartTime,
			end:   fileEndTime,
		})
		skip = true
		return nil
	}
	for _, file := range files {
		err := walkFn(filepath.Join(logDir, file.Name()), file)
		if err != nil {
			return nil, err
		}
	}
	// Sort by start time
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].start.Before(logFiles[j].start)
	})
	return logFiles, err
}

func (e *mctechLargeQueryRetriever) getFileStartTime(ctx context.Context, file *os.File) (time.Time, error) {
	var t time.Time
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return t, err
	}
	reader := bufio.NewReader(file)
	maxNum := 128
	for {
		lineByte, err := getOneLine(reader)
		if err != nil {
			return t, err
		}
		line := string(lineByte)
		if strings.HasPrefix(line, variable.MCTechLargeQueryStartPrefixStr) {
			return ParseTime(line[len(variable.MCTechLargeQueryStartPrefixStr):])
		}
		maxNum -= 1
		if maxNum <= 0 {
			break
		}
		if isCtxDone(ctx) {
			return t, ctx.Err()
		}
	}
	return t, errors.Errorf("malform slow query file %v", file.Name())
}

func (e *mctechLargeQueryRetriever) getRuntimeStats() execdetails.RuntimeStats {
	return e.stats
}

type mctechLargeQueryRuntimeStats struct {
	totalFileNum int
	readFileNum  int
	readFile     time.Duration
	initialize   time.Duration
	readFileSize int64
	parseLog     int64
	concurrent   int
}

// String implements the RuntimeStats interface.
func (s *mctechLargeQueryRuntimeStats) String() string {
	return fmt.Sprintf("initialize: %s, read_file: %s, parse_log: {time:%s, concurrency:%v}, total_file: %v, read_file: %v, read_size: %s",
		execdetails.FormatDuration(s.initialize), execdetails.FormatDuration(s.readFile),
		execdetails.FormatDuration(time.Duration(s.parseLog)), s.concurrent,
		s.totalFileNum, s.readFileNum, memory.FormatBytes(s.readFileSize))
}

// Merge implements the RuntimeStats interface.
func (s *mctechLargeQueryRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	tmp, ok := rs.(*mctechLargeQueryRuntimeStats)
	if !ok {
		return
	}
	s.totalFileNum += tmp.totalFileNum
	s.readFileNum += tmp.readFileNum
	s.readFile += tmp.readFile
	s.initialize += tmp.initialize
	s.readFileSize += tmp.readFileSize
	s.parseLog += tmp.parseLog
}

// Clone implements the RuntimeStats interface.
func (s *mctechLargeQueryRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := *s
	return &newRs
}

// Tp implements the RuntimeStats interface.
func (s *mctechLargeQueryRuntimeStats) Tp() int {
	return execdetails.TpMCTechLargeQueryRuntimeStat
}

func (e *mctechLargeQueryRetriever) getFileEndTime(ctx context.Context, file *os.File) (time.Time, error) {
	var t time.Time
	var tried int
	stat, err := file.Stat()
	if err != nil {
		return t, err
	}
	endCursor := stat.Size()
	maxLineNum := 128
	for {
		lines, readBytes, err := readLastLines(ctx, file, endCursor)
		if err != nil {
			return t, err
		}
		// read out the file
		if readBytes == 0 {
			break
		}
		endCursor -= int64(readBytes)
		for i := len(lines) - 1; i >= 0; i-- {
			if strings.HasPrefix(lines[i], variable.MCTechLargeQueryStartPrefixStr) {
				return ParseTime(lines[i][len(variable.MCTechLargeQueryStartPrefixStr):])
			}
		}
		tried += len(lines)
		if tried >= maxLineNum {
			break
		}
		if isCtxDone(ctx) {
			return t, ctx.Err()
		}
	}
	return t, errors.Errorf("invalid slow query file %v", file.Name())
}

func (e *mctechLargeQueryRetriever) initializeAsyncParsing(ctx context.Context, sctx sessionctx.Context) {
	e.taskList = make(chan mctechLargeQueryTask, 1)
	e.wg.Add(1)
	go e.parseDataForLargeQuery(ctx, sctx)
}

func (e *mctechLargeQueryRetriever) memConsume(bytes int64) {
	if e.memTracker != nil {
		e.memTracker.Consume(bytes)
	}
}

type mctechLargeQueryColumnValueFactory func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (valid bool, err error)

func getLargeQueryColumnValueFactoryByName(sctx sessionctx.Context, colName string, columnIdx int) (mctechLargeQueryColumnValueFactory, error) {
	switch colName {
	case variable.MCTechLargeQueryTimeStr:
		return func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (bool, error) {
			t, err := ParseTime(value)
			if err != nil {
				return false, err
			}
			timeValue := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, types.MaxFsp)
			if checker != nil {
				valid := checker.isTimeValid(timeValue)
				if !valid {
					return valid, nil
				}
			}
			if t.Location() != tz {
				t = t.In(tz)
				timeValue = types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, types.MaxFsp)
			}
			row[columnIdx] = types.NewTimeDatum(timeValue)
			return true, nil
		}, nil
	case variable.MCTechLargeQueryPlan:
		return func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (bool, error) {
			plan := parsePlan(value)
			row[columnIdx] = types.NewStringDatum(plan)
			return true, nil
		}, nil
	case execdetails.MCTechWriteKeysStr, execdetails.MCTechWriteSizeStr, execdetails.MCTechTotalKeysStr:
		return func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (valid bool, err error) {
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewUintDatum(v)
			return true, nil
		}, nil
	case variable.MCTechLargeQueryQueryTimeStr, variable.MCTechLargeQueryParseTimeStr,
		variable.MCTechLargeQueryCompileTimeStr, variable.MCTechLargeQueryRewriteTimeStr,
		variable.MCTechLargeQueryOptimizeTimeStr, execdetails.MCTechCopTimeStr,
		execdetails.MCTechProcessTimeStr, execdetails.MCTechWaitTimeStr:
		return func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (valid bool, err error) {
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewFloat64Datum(v)
			return true, nil
		}, nil
	case variable.MCTechLargeQueryUserStr, variable.MCTechLargeQueryHostStr,
		variable.MCTechLargeQueryDBStr, variable.MCTechLargeQueryDigestStr,
		variable.MCTechLargeQuerySQLStr:
		return func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (valid bool, err error) {
			row[columnIdx] = types.NewStringDatum(value)
			return true, nil
		}, nil
	case variable.MCTechLargeQueryMemMax, variable.MCTechLargeQueryDiskMax,
		variable.MCTechLargeQueryResultRows:
		return func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (valid bool, err error) {
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewIntDatum(v)
			return true, nil
		}, nil
	case variable.MCTechLargeQuerySuccStr:
		return func(row []types.Datum, value string, tz *time.Location, checker *mctechLargeQueryChecker) (valid bool, err error) {
			v, err := strconv.ParseBool(value)
			if err != nil {
				return false, err
			}
			row[columnIdx] = types.NewDatum(v)
			return true, nil
		}, nil
	}
	return nil, nil
}

// SaveLargeQuery is used to print the large query in the log files.
func (a *ExecStmt) SaveLargeQuery(succ bool) {
	sessVars := a.Ctx.GetSessionVars()
	cfg := config.GetMCTechConfig()
	threshold := cfg.Metrics.LargeQuery.Threshold
	enable := cfg.Metrics.LargeQuery.Enabled

	if !enable || len(a.StmtNode.OriginalText()) < threshold {
		// 不记录
		return
	}

	_, digest := sessVars.StmtCtx.SQLDigest()
	var stmtDetail execdetails.StmtExecDetails
	stmtDetailRaw := a.GoCtx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
	}
	execDetail := sessVars.StmtCtx.GetExecDetails()
	memMax := sessVars.StmtCtx.MemTracker.MaxConsumed()
	diskMax := sessVars.StmtCtx.DiskTracker.MaxConsumed()
	sql := a.GetTextToLog()
	costTime := time.Since(sessVars.StartTime) + sessVars.DurationParse
	largeItems := &variable.MCTechLargeQueryLogItems{
		SQL:               sql,
		Digest:            digest.String(),
		TimeTotal:         costTime,
		TimeParse:         sessVars.DurationParse,
		TimeCompile:       sessVars.DurationCompile,
		TimeOptimize:      sessVars.DurationOptimization,
		RewriteInfo:       sessVars.RewritePhaseInfo,
		ExecDetail:        execDetail,
		MemMax:            memMax,
		DiskMax:           diskMax,
		Succ:              succ,
		Plan:              getPlanTree(a.Ctx, a.Plan),
		WriteSQLRespTotal: stmtDetail.WriteSQLRespDuration,
		ResultRows:        GetResultRowsCount(a.Ctx, a.Plan),
	}
	slowLog := sessVars.LargeQueryFormat(largeItems)
	mctech.L().Warn(slowLog)
}
