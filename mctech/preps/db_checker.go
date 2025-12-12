package preps

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mctech"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

type databaseGrouper struct {
	groups []string
}

func newDatabaseGrouper(groups []string) *databaseGrouper {
	return &databaseGrouper{
		groups: append(make([]string, 0, len(groups)), groups...),
	}
}

// MatchGroup 检查给定的数据库名称是否完全包含于某个分组中
// dbNames: 要分组的数据库称表
// specialGroups: 除了初始默认的分组条件外，额外传入的只对本次生效的分组条件
func (g *databaseGrouper) MatchGroup(dbNames []string, specialGroup string) bool {
	gps := g.groups
	if len(specialGroup) > 0 {
		gps = append([]string{}, gps...)
		gps = append(gps, specialGroup)
	}

	// 先处理在分组中的数据库
	for _, gp := range gps {
		match := true
		for _, dbName := range dbNames {
			if !strings.Contains(gp, dbName) {
				// 不包含在当前规则中
				match = false
				break
			}
		}

		if match {
			// 当前规则不能全包含所有用到的数据库
			return true
		}
	}

	return false
}

// StmtTextAware stmt aware
type StmtTextAware interface {
	OriginalText() string
}

// DatabaseChecker interface
type DatabaseChecker interface {
	Check(mctx mctech.Context, aware StmtTextAware, dbs []string) error
}

// mutexDatabaseChecker 检查sql中用到的数据库是否存在互斥的库名
/**
 *
 * 检查的逻辑是先根据mutexFilters查询出互斥的数据库范围
 * 再根据excludeFilters查询出需要排除的数据库
 * 最后合并分组，同一分组内的数据库可以互相访问
 */
type mutexDatabaseChecker struct {
	mutexFilters   []mctech.Filter
	excludeFilters []mctech.Filter
	acrossGrouper  *databaseGrouper
}

var dbChecker *mutexDatabaseChecker
var dbCheckerInitOne sync.Once

func getDatabaseChecker() DatabaseChecker {
	if dbChecker != nil {
		return dbChecker
	}

	dbCheckerInitOne.Do(func() {
		option := config.GetMCTechConfig()
		dbChecker = newMutexDatabaseCheckerWithParams(
			option.DbChecker.Mutex,
			option.DbChecker.Exclude,
			option.DbChecker.Across)
	})
	return dbChecker
}

func newMutexDatabaseCheckerWithParams(mutex, exclude, across []string) *mutexDatabaseChecker {
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

	return &mutexDatabaseChecker{
		mutexFilters:   mutexFilters,
		excludeFilters: excludeFilters,
		acrossGrouper:  newDatabaseGrouper(across),
	}
}

func (c *mutexDatabaseChecker) Check(mctx mctech.Context, aware StmtTextAware, dbs []string) error {
	result := mctx.PrepareResult()
	if !result.Roles().TenantOnly() {
		return nil
	}

	if checkExcepts(result) {
		return nil
	}

	logicNames := make([]string, 0, len(dbs))
	for _, dbName := range dbs {
		logicName := mctx.ToLogicDbName(dbName)
		matched := c.dbPredicate(logicName)

		if matched && !slices.Contains(logicNames, logicName) {
			logicNames = append(logicNames, logicName)
		}
	}

	var specialGroup string
	if result := mctx.PrepareResult(); result != nil {
		params := result.Params()
		if v, ok := params[mctech.ParamAcross]; ok {
			var across string
			if across, ok = v.(string); !ok {
				return errors.New("'across'参数类型必须是字符串")
			}
			specialGroup = across
		}
	}

	failpoint.Inject("DbCheckError", func(_ failpoint.Value) {
		dbs := []string{"global_ec5", "global_sq"}
		logicNames = append(logicNames, dbs...)
	})

	if len(logicNames) <= 1 {
		// 数据库只有一个
		return nil
	}

	if match := c.acrossGrouper.MatchGroup(logicNames, specialGroup); match {
		// 数据库全部属于一个分组
		return nil
	}

	msg := fmt.Sprintf("dbs not allow in the same statement. %s", logicNames)
	sql := aware.OriginalText()
	maxQueryLen := 1024
	length := len(sql)
	if length > maxQueryLen {
		sql = fmt.Sprintf("%.*s......(len:%d)", maxQueryLen, sql, length)
	}
	log.Warn(msg, zap.String("SQL", sql))
	return errors.New(msg)
}

func (c *mutexDatabaseChecker) dbPredicate(dbName string) bool {
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

func checkExcepts(result mctech.PrepareResult) bool {
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
