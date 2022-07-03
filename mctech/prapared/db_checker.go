package prapared

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/mctech"
	"golang.org/x/exp/slices"
)

type DbGroup interface {
	groupSet() // 无实际意义的接口方法，仅仅为了表示实现了DbGroup接口
}

type MultiDbGroup struct {
	dbNames []string
}

func (g *MultiDbGroup) groupSet() {
}

func (g *MultiDbGroup) String() string {
	return "{" + strings.Join(g.dbNames, ",") + "}"
}

type SingleDbGroup struct {
	dbName string
}

func (g *SingleDbGroup) groupSet() {
}

func (g *SingleDbGroup) String() string {
	return g.dbName
}

type DatabaseGrouper struct {
	groups []string
}

func NewDatabaseGrouper(groups []string) *DatabaseGrouper {
	return &DatabaseGrouper{
		groups: append(make([]string, 0, len(groups)), groups...),
	}
}

func (g *DatabaseGrouper) GroupBy(context mctech.MCTechContext, dbNames []string) []DbGroup {
	// 合并前数据库只有一个
	results := make([]DbGroup, 0, 10)
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
				results = append(results, &MultiDbGroup{dbNames: lst})
			} else {
				results = append(results, &SingleDbGroup{dbName: lst[0]})
			}
		}
	}

	for _, dbName := range dbNames {
		// 排除前面在分组数据库中已处理的
		if _, ok := used[dbName]; ok {
			continue
		}

		results = append(results, &SingleDbGroup{dbName: dbName})
	}

	return results
}

type StringFilter struct {
	pattern string
	action  string
}

func NewStringFilter(pattern string) *StringFilter {
	index := strings.Index(pattern, ":")
	filter := &StringFilter{}
	if index > 0 {
		filter.action = pattern[0:index]
		filter.pattern = pattern[index+1:]
	} else {
		filter.action = ""
		filter.pattern = pattern
	}

	if filter.action == "regex" {
		filter.pattern = "(?i)" + filter.pattern
	}
	return filter
}

func (f *StringFilter) Match(text string) (bool, error) {
	var (
		matched = true
		err     error
	)
	switch f.action {
	case "starts-with":
		matched = strings.HasPrefix(text, f.pattern)
	case "ends-with":
		matched = strings.HasSuffix(text, f.pattern)
	case "contains":
		matched = strings.Contains(text, f.pattern)
	case "regex":
		matched, err = regexp.MatchString(f.pattern, text)
	default:
		matched = f.pattern == text
	}
	return matched, err
}

/**
 * 检查sql中用到的数据库是否存在互斥的库名
 * <p>
 * 检查的逻辑是先根据mutexFilters查询出互斥的数据库范围
 * 再根据excludeFilters查询出需要排除的数据库
 * 最后合并分组，同一分组内的数据库可以互相访问
 */
type MutexDatabaseChecker struct {
	mutexFilters   []*StringFilter
	excludeFilters []*StringFilter
	grouper        *DatabaseGrouper
}

func NewMutexDatabaseChecker() *MutexDatabaseChecker {
	option := mctech.GetOption()
	mutexAcrossDbs := option.DbChecker_MutexAcrossDbs
	mutexFilters := []*StringFilter{
		NewStringFilter("starts-with:global_"),
	}

	if len(mutexAcrossDbs) > 0 {
		for _, t := range mutexAcrossDbs {
			mutexFilters = append(mutexFilters, NewStringFilter(t))
		}
	}

	// 在mutex filters过滤结果中中可与其它数据库共同查询的表
	excludeFilters := []*StringFilter{
		NewStringFilter("global_platform"),
		NewStringFilter("global_ipm"),
		NewStringFilter("starts-with:global_dw_"),
		NewStringFilter("global_dwb"),
	}

	excludeAcrossDbs := option.DbChecker_ExcludeAcrossDbs
	if len(excludeAcrossDbs) > 0 {
		for _, t := range excludeAcrossDbs {
			excludeFilters = append(excludeFilters, NewStringFilter(t))
		}
	}

	groupDbs := option.DbChecker_AcrossDbGroups
	groups := []string{"global_mtlp|global_ma"}
	if len(groupDbs) > 0 {
		groups = append(groups, groupDbs...)
	}

	return &MutexDatabaseChecker{
		mutexFilters:   mutexFilters,
		excludeFilters: excludeFilters,
		grouper:        NewDatabaseGrouper(groups),
	}
}

func (c *MutexDatabaseChecker) Check(context mctech.MCTechContext, dbs []string) error {
	result := context.ResolveResult()
	if !result.TenantFromRole() {
		return nil
	}

	logicNames := make([]string, 0, len(dbs))
	for _, dbName := range dbs {
		logicName := context.ToLogicDbName(dbName)
		matched, err := c.dbPredicate(logicName)
		if err != nil {
			return err
		}

		if matched {
			logicNames = append(logicNames, logicName)
		}
	}
	groupDbs := c.grouper.GroupBy(context, logicNames)
	// 合并后数据库只有一个
	if len(groupDbs) <= 1 {
		return nil
	}

	return fmt.Errorf("dbs not allow in the same statement.  %s", groupDbs)
}

func (c *MutexDatabaseChecker) dbPredicate(dbName string) (bool, error) {
	for _, f := range c.mutexFilters {
		matched, err := f.Match(dbName)
		if err != nil {
			return false, err
		}

		if matched {
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
