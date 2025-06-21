package preps

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/mctech"
	mcworker "github.com/pingcap/tidb/pkg/mctech/worker"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

func createGlobalList(groups []string) []*mcworker.CrossDBInfo {
	if len(groups) == 0 {
		return nil
	}

	list := make([]*mcworker.CrossDBInfo, 0, len(groups))
	for _, item := range groups {
		list = append(list, &mcworker.CrossDBInfo{
			Groups: []mcworker.CrossDBGroup{
				{DBList: config.StrToSlice(item, "|")},
			},
		})
	}
	return list
}

// StmtTextAware stmt aware
type StmtTextAware interface {
	OriginalText() string
}

// DatabaseChecker interface
type DatabaseChecker interface {
	Check(mctx mctech.Context, aware StmtTextAware, schema mctech.StmtSchemaInfo) error
}

// mutuallyExclusiveDatabaseChecker 检查sql中用到的数据库是否存在互斥的库名
/**
 *
 * 检查的逻辑是先根据mutexFilters查询出互斥的数据库范围
 * 再根据excludeFilters查询出需要排除的数据库
 * 最后合并分组，同一分组内的数据库可以互相访问
 */
type mutuallyExclusiveDatabaseChecker struct {
	mutexFilters   []mctech.Filter
	excludeFilters []mctech.Filter
	globalList     []*mcworker.CrossDBInfo
}

var dbChecker *mutuallyExclusiveDatabaseChecker
var dbCheckerInitOne sync.Once

func getDatabaseChecker() DatabaseChecker {
	if dbChecker != nil {
		return dbChecker
	}

	dbCheckerInitOne.Do(func() {
		option := config.GetMCTechConfig()
		dbChecker = newMutuallyExclusiveDatabaseCheckerWithParams(
			option.DbChecker.Mutex,
			option.DbChecker.Exclude,
			option.DbChecker.Across)
	})
	return dbChecker
}

func newMutuallyExclusiveDatabaseCheckerWithParams(mutex, exclude, across []string) *mutuallyExclusiveDatabaseChecker {
	var mutexFilters []mctech.Filter
	for _, t := range mutex {
		if filter, ok := mctech.NewStringFilter(t); ok {
			mutexFilters = append(mutexFilters, filter)
		}
	}

	// 在mutex filters过滤结果中中可与其它数据库共同查询的表
	var excludeFilters []mctech.Filter
	for _, t := range exclude {
		if filter, ok := mctech.NewStringFilter(t); ok {
			excludeFilters = append(excludeFilters, filter)
		}
	}

	return &mutuallyExclusiveDatabaseChecker{
		mutexFilters:   mutexFilters,
		excludeFilters: excludeFilters,
		globalList:     createGlobalList(across),
	}
}

func (c *mutuallyExclusiveDatabaseChecker) getLogicDBNamesForCrossDBCheck(mctx mctech.Context, schema mctech.StmtSchemaInfo) []string {
	failpoint.Inject("DbCheckError", func(_ failpoint.Value) {
		failpoint.Return([]string{"global_ec5", "global_sq"})
	})

	logicDBNames := make([]string, 0, len(schema.Databases))
	for _, dbName := range schema.Databases {
		logicDBName := mctx.ToLogicDbName(dbName)
		matched := c.dbPredicate(logicDBName)

		if matched && !slices.Contains(logicDBNames, logicDBName) {
			// logicDBName是需要检查跨sql的数据库
			logicDBNames = append(logicDBNames, logicDBName)
		}
	}

	return logicDBNames
}

func (c *mutuallyExclusiveDatabaseChecker) Check(mctx mctech.Context, aware StmtTextAware, schema mctech.StmtSchemaInfo) error {
	result := mctx.ParseResult()
	if !result.Roles().TenantOnly() {
		return nil
	}

	if checkExcepts(result) {
		return nil
	}

	logicDBNames := c.getLogicDBNamesForCrossDBCheck(mctx, schema)
	if len(logicDBNames) <= 1 {
		// 数据库只有一个
		return nil
	}

	if ok, err := c.checkCrossDBs(mctx.Session(), result, logicDBNames); err != nil || ok {
		// 出现了错识或者通过了检查
		return err
	}

	msg := fmt.Sprintf("dbs not allow in the same statement. %s", logicDBNames)
	sql := aware.OriginalText()
	maxQueryLen := 1024
	length := len(sql)
	if length > maxQueryLen {
		sql = fmt.Sprintf("%.*s......(len:%d)", maxQueryLen, sql, length)
	}
	log.Warn(msg, zap.String("SQL", sql))
	return errors.New(msg)
}

func (c *mutuallyExclusiveDatabaseChecker) checkByCrossDBInfo(ctx sessionctx.Context, pattern mcworker.SQLInvokerPattern, checkCb func(*mcworker.CrossDBInfo) bool) (bool, error) {
	var (
		info *mcworker.CrossDBInfo
		err  error
	)

	if info, err = getCrossDBInfo(ctx, pattern); err != nil {
		return false, fmt.Errorf("get CrossDBInfo error. type: %s, name: %s. %w", mcworker.AllInvokerTypes[pattern.Type()], pattern.Name(), err)
	}

	if info != nil {
		// 找到合适的规则，执行检查回调函数
		if ok := checkCb(info); ok {
			return true, nil
		}
	}

	if pattern.MatchAny() {
		return false, nil
	}

	// 传入的精确匹配，并且没有通过检查。此时再转换成通配符匹配模式再检查一次
	return c.checkByCrossDBInfo(ctx,
		mcworker.NewSQLInvokerPattern(mcworker.MatchAnyInvoker, pattern.Type()),
		checkCb)
}

// dbPredicate 判断给定的数据名是否属于需要检查跨库sql的数据库
func (c *mutuallyExclusiveDatabaseChecker) dbPredicate(dbName string) bool {
	for _, deny := range c.mutexFilters {
		matched := deny.Match(dbName)
		if !matched {
			continue
		}

		for _, f := range c.excludeFilters {
			matched := f.Match(dbName)
			if matched {
				// 需要排除，不当作互斥数据库处理
				return false
			}
		}

		// 符合互斥数据库
		return true
	}
	// 不符合互斥数据库
	return false
}

// checkCrossDBs 检查给定的数据库名称是否完全包含于某个分组中
func (c *mutuallyExclusiveDatabaseChecker) checkCrossDBs(ctx sessionctx.Context, result mctech.ParseResult, logicDBNames []string) (bool, error) {
	// 额外传入的只对本次生效的分组条件
	var specialGroup []string
	if v, ok := result.GetParam(mctech.ParamAcross); ok {
		var across string
		if across, ok = v.(string); !ok {
			return false, errors.New("'across'参数类型必须是'|'分隔的字符串")
		}
		specialGroup = config.StrToSlice(across, "|")
	}
	if len(specialGroup) > 0 {
		specialInfo := &mcworker.CrossDBInfo{
			Groups: []mcworker.CrossDBGroup{
				{DBList: specialGroup},
			},
		}
		// sql中显示指定的需要跨库查询的数据库列表
		if c.checkBySingleCrossDBInfo(logicDBNames, specialInfo) {
			// 数据库全部属于一个分组
			return true, nil
		}
	}

	// 先匹配公共分组中的数据库组
	for _, info := range c.globalList {
		// 配置文件中指定的全局可以跨库查询的数据库列表
		if c.checkBySingleCrossDBInfo(logicDBNames, info) {
			// 当前规则能够全部包含所有用到的数据库
			return true, nil
		}
	}

	var checkCb = func(info *mcworker.CrossDBInfo) bool {
		// 再匹配当前服务识别到的数据库组
		if info.AllowAllDBs {
			// 允许当前服务所有库跨库查询
			return true
		}
		return c.checkBySingleCrossDBInfo(logicDBNames, info)
	}

	var (
		pattern mcworker.SQLInvokerPattern
		svcName string
		pkgName string
	)
	comments := result.Comments()
	// 通过服务名称获取到的跨库查询的数据库列表信息
	if svc := comments.Service(); svc != nil {
		svcName = svc.From()
	}
	pattern = mcworker.NewSQLInvokerPattern(svcName, mcworker.InvokerTypeService)
	if ok, err := c.checkByCrossDBInfo(ctx, pattern, checkCb); err != nil || ok {
		return ok, err
	}

	// 通过包名称获取到的跨库查询的数据库列表信息
	if pkg := comments.Package(); pkg != nil {
		pkgName = pkg.Name()
	}
	pattern = mcworker.NewSQLInvokerPattern(pkgName, mcworker.InvokerTypePackage)
	if ok, err := c.checkByCrossDBInfo(ctx, pattern, checkCb); err != nil || ok {
		return ok, err
	}

	// 没有通过检查
	return false, nil
}

func (c *mutuallyExclusiveDatabaseChecker) checkBySingleCrossDBInfo(logicDBNames []string, info *mcworker.CrossDBInfo) bool {
	for _, gp := range info.Groups {
		allPass := true
		for _, dbName := range logicDBNames {
			if !slices.Contains(gp.DBList, dbName) {
				// 不包含在当前规则中
				allPass = false
				break
			}
		}

		// 所有的库名称都通过了规则检查
		if allPass {
			return true
		}
	}

	// 没有一条规则能满足传入的库名称列表
	return false
}

func getCrossDBInfo(ctx sessionctx.Context, pattern mcworker.SQLInvokerPattern) (*mcworker.CrossDBInfo, error) {
	dom := domain.GetDomain(ctx)
	var (
		mgr domain.CrossDBManager
		ok  bool
	)
	if mgr, ok = dom.CrossDBManager(); !ok {
		if !intest.InTest {
			return nil, errors.New("Domain.crossDBManager is nil")
		}
		return nil, nil
	}

	return mgr.Get(pattern), nil
}

func checkExcepts(result mctech.ParseResult) bool {
	excepts := config.GetMCTechConfig().DbChecker.Excepts
	comments := result.Comments()
	for _, except := range excepts {
		pkg := comments.Package()
		if pkg != nil {
			// 依赖包名称
			if except == pkg.Name() {
				return true
			}
		}

		svc := comments.Service()
		if svc != nil {
			// 服务名称比较
			if strings.Contains(except, ".") {
				// 包含产品线的服务完整名称
				if svc.From() == except {
					return true
				}
			} else {
				// 只包含服务名称
				if svc.AppName() == except {
					return true
				}
			}
		}
	}

	return false
}
