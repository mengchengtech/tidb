package preps

import (
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/tidb/mctech"
	"golang.org/x/exp/slices"
)

type dbGroup interface {
	groupSet() // 无实际意义的接口方法，仅仅为了表示实现了DbGroup接口
}

type multiDbGroup struct {
	dbNames []string
}

func (g *multiDbGroup) groupSet() {
}

func (g *multiDbGroup) String() string {
	return "{" + strings.Join(g.dbNames, ",") + "}"
}

type singleDbGroup struct {
	dbName string
}

func (g *singleDbGroup) groupSet() {
}

func (g *singleDbGroup) String() string {
	return g.dbName
}

type databaseGrouper struct {
	groups []string
}

func newDatabaseGrouper(groups []string) *databaseGrouper {
	return &databaseGrouper{
		groups: append(make([]string, 0, len(groups)), groups...),
	}
}

func (g *databaseGrouper) GroupBy(dbNames []string) []dbGroup {
	// 合并前数据库只有一个
	results := make([]dbGroup, 0, 10)
	used := map[string]bool{}

	// 先处理在分组中的数据库
	for _, gp := range g.groups {
		var lst []string
		for _, dbName := range dbNames {
			if strings.Contains(gp, dbName) {
				// 包含在当前分组中
				if lst == nil {
					lst = make([]string, 0, 10)
				}
				used[dbName] = true
				lst = append(lst, dbName)
			}
		}

		if lst != nil {
			if len(lst) > 1 {
				slices.SortFunc(lst, func(a, b string) bool {
					return strings.ToLower(a) < strings.ToLower(b)
				})
				results = append(results, &multiDbGroup{dbNames: lst})
			} else {
				results = append(results, &singleDbGroup{dbName: lst[0]})
			}
		}
	}

	for _, dbName := range dbNames {
		// 排除前面在分组数据库中已处理的
		if _, ok := used[dbName]; ok {
			continue
		}

		results = append(results, &singleDbGroup{dbName: dbName})
	}

	return results
}

// DatabaseChecker interface
type DatabaseChecker interface {
	Check(mctx mctech.Context, dbs []string) error
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
	grouper        *databaseGrouper
}

var dbChecker *mutexDatabaseChecker
var dbCheckerInitOne sync.Once

func getDatabaseChecker() DatabaseChecker {
	if dbChecker != nil {
		return dbChecker
	}

	dbCheckerInitOne.Do(func() {
		option := mctech.GetOption()
		dbChecker = newMutexDatabaseCheckerWithParams(
			option.DbCheckerMutexAcrossDbs,
			option.DbCheckerExcludeAcrossDbs,
			option.DbCheckerAcrossDbGroups)
	})
	return dbChecker
}

func newMutexDatabaseCheckerWithParams(mutexAcrossDbs, excludeAcrossDbs, groupDbs []string) *mutexDatabaseChecker {
	mutexAcrossDbs = append(
		slices.Clone(mutexAcrossDbs),
		mctech.PrefixFilterPattern(mctech.DbGlobalPrefix),
		mctech.PrefixFilterPattern(mctech.DbAssetPrefix),
		mctech.PrefixFilterPattern(mctech.DbPublicPrefix),
	)
	mutexFilters := make([]mctech.Filter, len(mutexAcrossDbs))
	for i, t := range mutexAcrossDbs {
		mutexFilters[i] = mctech.NewStringFilter(t)
	}

	// 在mutex filters过滤结果中中可与其它数据库共同查询的表
	excludeAcrossDbs = append(slices.Clone(excludeAcrossDbs),
		"global_platform", "global_ipm", "starts-with:global_dw_", "global_dwb")
	excludeFilters := make([]mctech.Filter, len(excludeAcrossDbs))
	for i, t := range excludeAcrossDbs {
		excludeFilters[i] = mctech.NewStringFilter(t)
	}

	// 数据库分组，组内的数据库可以互机访问
	groups := []string{"global_mtlp|global_ma"}
	if len(groupDbs) > 0 {
		groups = append(groups, groupDbs...)
	}

	return &mutexDatabaseChecker{
		mutexFilters:   mutexFilters,
		excludeFilters: excludeFilters,
		grouper:        newDatabaseGrouper(groups),
	}
}

func (c *mutexDatabaseChecker) Check(mctx mctech.Context, dbs []string) error {
	result := mctx.PrepareResult()
	if !result.TenantFromRole() {
		return nil
	}

	logicNames := make([]string, 0, len(dbs))
	for _, dbName := range dbs {
		logicName := mctx.ToLogicDbName(dbName)
		matched, err := c.dbPredicate(logicName)
		if err != nil {
			return err
		}

		if matched {
			logicNames = append(logicNames, logicName)
		}
	}
	groupDbs := c.grouper.GroupBy(logicNames)
	// 合并后数据库只有一个
	if len(groupDbs) <= 1 {
		return nil
	}

	return fmt.Errorf("dbs not allow in the same statement.  %s", groupDbs)
}

func (c *mutexDatabaseChecker) dbPredicate(dbName string) (bool, error) {
	for _, deny := range c.mutexFilters {
		matched, err := deny.Match(dbName)
		if err != nil {
			return false, err
		}

		if !matched {
			continue
		}

		for _, f := range c.excludeFilters {
			matched, err := f.Match(dbName)
			if err != nil {
				return false, err
			}

			if matched {
				// 需要排除，不当作互斥数据库处理
				return false, nil
			}
		}

		// 符合互斥数据库
		return true, nil
	}
	// 不符合互斥数据库
	return false, nil
}
