package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/mctech"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	// MCTechCrossDB is a table name
	MCTechCrossDB = "mctech_cross_db"
	// CreateMCTechCrossDB is a table about cross db
	CreateMCTechCrossDB = `CREATE TABLE IF NOT EXISTS %n.%n (
		id bigint not null,
	  invoker_name varchar(128) not null comment '执行sql的服务的全名称(格式为 {service_name}.{product_line})或所属依赖包的名称。区分大小写',
	  invoker_type enum('service', 'package', 'both') not null comment 'invoker_name的代表的类型。其中 ''both'' 表示同时支持''service'' 和 ''package''',
		allow_all_dbs boolean not null comment '是否允许所有的数据都可以跨库执行sql',
		cross_dbs varchar(1024) not null comment '当allow_all_dbs为false时，允许跨库执行sql的数据库列表('',''分隔)。列表中只要有一个''*''，则其它不为''*''的数据库就被当作不受跨库限制',
		enabled boolean not null comment '当前规则是否启用',
		created_at datetime not null,
    remark varchar(512),
		primary key (id)
	);`

	// InitMCTechCrossDBData0 init mctech_cross_db table data
	InitMCTechCrossDBData0 = `insert into %n.%n
		(id, invoker_name, invoker_type, allow_all_dbs, cross_dbs, enabled, created_at, remark)
		values (1, '*', 'both', false, 'global_mtlp,global_ma', true, now(), '同一条sql语句中允许同时使用给定的数据库')
		;`

	// InitMCTechCrossDBData1 init mctech_cross_db table data
	InitMCTechCrossDBData1 = `insert into %n.%n
		(id, invoker_name, invoker_type, allow_all_dbs, cross_dbs, enabled, created_at, remark)
		values (2, '*', 'both', false, 'global_platform,global_ipm,*', true, now(), '规则里其中一项为''*''时，其它数据库排除在任意规则检查之外')
		,(3, '*', 'both', false, 'global_dw_*,global_dwb,*', true, now(), '规则里其中一项为''*''时，其它数据库排除在任意规则检查之外')
		,(4, '@mctech/dp-impala-tidb-enhanced', 'package', true, '', true, now(), '删除约束检查里跨库约束规则检查，需要允许任意配置的跨库规则')
		;`

	selectCrossDBSQL = "SELECT id, invoker_name, invoker_type, allow_all_dbs, cross_dbs, enabled, remark from %n.%n"
)

// InvokerType invoker type. such as 'service', 'package' or 'both'
type InvokerType int32

const (
	// InvokerTypeService 代表给定的Invoker是服务类型
	InvokerTypeService InvokerType = iota
	// InvokerTypePackage 代表给定的Invoker是依赖包类型
	InvokerTypePackage
	// InvokerTypeBoth 代表给定的Invoker既可以是服务类型也可以是依赖包类型
	InvokerTypeBoth
)

// AllInvokerTypes all state types. the order MUST adapt to [InvokerType] type
var AllInvokerTypes = []string{"service", "package", "both"}

// ToTypes change to slices
func (t InvokerType) ToTypes() []InvokerType {
	var types []InvokerType
	if t == InvokerTypeBoth {
		types = []InvokerType{InvokerTypeService, InvokerTypePackage}
	} else {
		types = []InvokerType{t}
	}

	return types
}

// ToEnum change current value to tidb enum value
func (t InvokerType) ToEnum() types.Enum {
	e, err := types.ParseEnumValue(AllInvokerTypes, uint64(t+1))
	if err != nil {
		panic("this should never happen")
	}
	return e
}

// ResultStateType loaded result state
type ResultStateType int32

const (
	// ResultStateTypeSuccess state success
	ResultStateTypeSuccess ResultStateType = iota
	// ResultStateTypeDisabled state disabled
	ResultStateTypeDisabled
	// ResultStateTypeError state error
	ResultStateTypeError
)

// AllResultStateTypes all state types. the order MUST adapt to [ResultStateType] type
var AllResultStateTypes = []string{"success", "disabled", "error"}

// ToEnum change current value to tidb enum value
func (t ResultStateType) ToEnum() types.Enum {
	e, err := types.ParseEnumValue(AllResultStateTypes, uint64(t+1))
	if err != nil {
		panic("this should never happen")
	}
	return e
}

const (
	// MatchAnyInvoker define string that match any invoker name
	MatchAnyInvoker = "*"
	// MatchAnyDB define string that match any db name
	MatchAnyDB = "*"
)

const crossDBManagerLoopTickerInterval = 30 * time.Second

// SQLInvokerPatternContext sql invoker pattern context
type SQLInvokerPatternContext interface {
	// GetPatterns returns all patterns
	GetPatterns() []SQLInvokerPattern
	// IsSame compare other instance
	IsSame(other SQLInvokerPatternContext) bool
}

type sqlInvokerPatternContext struct {
	svcName string
	pkgName string
}

// NewSQLInvokerPatternContext create instance implements SQLInvokerPatternContext interface
func NewSQLInvokerPatternContext(comments mctech.Comments) SQLInvokerPatternContext {
	var (
		svcName string
		pkgName string
	)
	// 通过服务名称获取到的跨库查询的数据库列表信息
	if svc := comments.Service(); svc != nil {
		svcName = svc.From()
		if svcName == "" {
			svcName = MatchAnyInvoker
		}
	}
	// 通过包名称获取到的跨库查询的数据库列表信息
	if pkg := comments.Package(); pkg != nil {
		pkgName = pkg.Name()
		if pkgName == "" {
			pkgName = MatchAnyInvoker
		}
	}
	return &sqlInvokerPatternContext{svcName: svcName, pkgName: pkgName}
}

func (c *sqlInvokerPatternContext) GetPatterns() []SQLInvokerPattern {
	patterns := make([]SQLInvokerPattern, 0, 10)
	patterns = append(patterns, &sqlInvokerPattern{name: c.svcName, tp: InvokerTypeService})
	if c.svcName != MatchAnyInvoker {
		patterns = append(patterns, &sqlInvokerPattern{name: MatchAnyInvoker, tp: InvokerTypeService})
	}
	patterns = append(patterns, &sqlInvokerPattern{name: c.pkgName, tp: InvokerTypePackage})
	if c.pkgName != MatchAnyInvoker {
		patterns = append(patterns, &sqlInvokerPattern{name: MatchAnyInvoker, tp: InvokerTypePackage})
	}
	return patterns
}

func (c *sqlInvokerPatternContext) IsSame(other SQLInvokerPatternContext) bool {
	var (
		b  *sqlInvokerPatternContext
		ok bool
	)
	if b, ok = other.(*sqlInvokerPatternContext); !ok {
		return false
	}
	return c.svcName == b.svcName && c.pkgName == b.pkgName
}

// SQLInvokerPattern sql invoker pattern
type SQLInvokerPattern interface {
	Name() string
	Type() InvokerType

	// CreateKey create key to map
	CreateKey() string
	// IsSame compare other instance
	IsSame(other SQLInvokerPattern) bool
	// MatchAny check is matched any sql invoker
	MatchAny() bool
}

type sqlInvokerPattern struct {
	name string
	tp   InvokerType
}

// NewSQLInvokerPattern create instance implements SQLInvokerPattern interface
func NewSQLInvokerPattern(name string, tp InvokerType) SQLInvokerPattern {
	if name == "" {
		name = MatchAnyInvoker
	}
	return &sqlInvokerPattern{name: name, tp: tp}
}

// Name method implements interface
func (s *sqlInvokerPattern) Name() string {
	return s.name
}

// Type method implements interface
func (s *sqlInvokerPattern) Type() InvokerType {
	return s.tp
}

// CreateKey method implements interface
func (s *sqlInvokerPattern) CreateKey() string {
	return fmt.Sprintf("%s@%s", s.name, AllInvokerTypes[s.tp])
}

// IsSame method implements interface
func (s *sqlInvokerPattern) IsSame(other SQLInvokerPattern) bool {
	if s == nil {
		return other == nil
	} else if other == nil {
		return false
	}
	return s.name == other.Name() && s.tp == other.Type()
}

// MatchAny method implements interface
func (s *sqlInvokerPattern) MatchAny() bool {
	return s.name == MatchAnyInvoker || s.name == ""
}

// CrossDBInfo 允许特定服务跨库查询
type CrossDBInfo struct {
	// 是否允许所有数据库都可以跨库执行sql语句
	AllowAllDBs bool
	// 允许跨库执行sql的数据库列表分组，检查时每组分别检查，组和组之间不合并
	Groups []CrossDBGroup
	// 排除在跨库执行sql检查之外的数据库列表。当 AllowAllDBs 值为 false 时，仍然不受任何跨库检查约束
	filters []mctech.Filter
}

// Exclude method filter db that excluded
func (c *CrossDBInfo) Exclude(dbNames []string) []string {
	return doFilterDbNames(c.filters, dbNames)
}

// CrossDBGroup 允许跨库查询的数据库组
type CrossDBGroup struct {
	ID int64
	// DBList 可以在同一个SQL中跨库查询的数据库列表
	DBList []string
}

// LoadedRuleResult load rule result struct
type LoadedRuleResult struct {
	ID          int64
	InvokerName string
	InvokerType InvokerType
	AllowAllDBs bool
	CrossDBs    string
	Enabled     bool
	Remark      string
	Data        LoadedRuleResultData
}

func (r *LoadedRuleResult) initDetail() *CrossDBDetailData {
	detail := &CrossDBDetailData{AllowAllDBs: r.AllowAllDBs}
	detail.init(r.InvokerName, r.InvokerType)
	r.Data.Detail = detail
	return detail
}

// LoadedRuleResultData load result data
type LoadedRuleResultData struct {
	State    ResultStateType
	Message  string
	LoadedAt time.Time
	Detail   *CrossDBDetailData
}

// SetState set state
func (d *LoadedRuleResultData) SetState(state ResultStateType, message string) {
	d.State = state
	d.Message = message
	d.LoadedAt = time.Now()
}

// CrossDBGroupData cross db group data
type CrossDBGroupData struct {
	DBList []string
}

// FilterData filter data
type FilterData struct {
	Global   bool
	Patterns []string
}

// CrossDBDetailData cross db detail data struct
type CrossDBDetailData struct {
	Service       string
	Package       string
	AllowAllDBs   bool
	Filters       *FilterData
	CrossDBGroups []CrossDBGroupData
}

func (c *CrossDBDetailData) init(name string, tp InvokerType) {
	switch tp {
	case InvokerTypeService:
		c.Service = name
	case InvokerTypePackage:
		c.Package = name
	case InvokerTypeBoth:
		c.Package = name
		c.Service = name
	}
}

type defaultCrossDBScheduler struct {
	ctx context.Context
	lck sync.RWMutex

	allowCrossInfos map[string]*CrossDBInfo
	// 全局排除在外的数据库过滤器（所有的外部调用端都适用）
	filters []mctech.Filter
	// 用于状态检查表
	loadedResults []*LoadedRuleResult
}

// Exclude method exclude db that matched
func (m *defaultCrossDBScheduler) Exclude(dbNames []string) []string {
	m.lck.Lock()
	defer m.lck.Unlock()

	if len(m.filters) == 0 {
		return dbNames
	}

	return doFilterDbNames(m.filters, dbNames)
}

// Get get CrossDBInfo
func (m *defaultCrossDBScheduler) Get(key string) *CrossDBInfo {
	m.lck.RLock()
	defer m.lck.RUnlock()

	if key == "" {
		key = MatchAnyInvoker
	}
	return m.allowCrossInfos[key]
}

func (m *defaultCrossDBScheduler) SetAll(all map[string]*CrossDBInfo) {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.allowCrossInfos = all
}

func (m *defaultCrossDBScheduler) GetAll() map[string]*CrossDBInfo {
	m.lck.RLock()
	defer m.lck.RUnlock()

	if m.allowCrossInfos == nil {
		return nil
	}
	return maps.Clone(m.allowCrossInfos)
}

func (m *defaultCrossDBScheduler) GetLoadedResults() []*LoadedRuleResult {
	m.lck.RLock()
	defer m.lck.RUnlock()

	if m.loadedResults == nil {
		return nil
	}
	return slices.Clone(m.loadedResults)
}

func (m *defaultCrossDBScheduler) ReloadAll(se sqlexec.SQLExecutor) error {
	rs, err := se.ExecuteInternal(m.ctx, selectCrossDBSQL, mysql.SystemDB, MCTechCrossDB)
	if err != nil || rs == nil {
		return err
	}

	defer func() {
		terror.Log(rs.Close())
	}()

	rows, err := sqlexec.DrainRecordSet(m.ctx, rs, 8)
	if err != nil {
		return err
	}

	var (
		newnewAllowCrossInfos = map[string]*CrossDBInfo{}
		newFilters            []mctech.Filter
		newLoadedResults      = make([]*LoadedRuleResult, 0, len(rows))
	)
	for _, row := range rows {
		result := &LoadedRuleResult{
			ID:          row.GetInt64(0),
			InvokerName: row.GetString(1),
			InvokerType: InvokerType(row.GetEnum(2).Value - 1),
			AllowAllDBs: row.GetInt64(3) != 0,
			CrossDBs:    row.GetString(4),
			Enabled:     row.GetInt64(5) != 0,
			Remark:      row.GetString(6),
		}

		var (
			filterMap map[string]mctech.Filter
			ok        bool
		)
		switch {
		case !result.Enabled:
			result.Data.SetState(ResultStateTypeDisabled, "current rule is Disabled")
		case len(result.InvokerName) == 0:
			result.Data.SetState(ResultStateTypeError, "Ignore. The 'invoker_name' field is empty.")
		case result.AllowAllDBs:
			if result.InvokerName == MatchAnyInvoker {
				result.Data.SetState(ResultStateTypeError, "Ignore. The 'allow_all_dbs' field should not be false, when invoker_name is '*'.")
			} else {
				result.Data.SetState(ResultStateTypeSuccess, "Loaded Success")
				result.initDetail()
				ok = true
			}
		default:
			if filterMap, ok = parseDBGroupsAndFilters(result); ok {
				// 存在有效的过滤器
				if len(filterMap) > 0 {
					fd := &FilterData{
						Global: result.InvokerName == MatchAnyInvoker && result.InvokerType == InvokerTypeBoth,
					}
					for pattern, filter := range filterMap {
						fd.Patterns = append(fd.Patterns, pattern)
						if fd.Global {
							newFilters = append(newFilters, filter)
						}
					}
					if len(fd.Patterns) > 1 {
						sort.Strings(fd.Patterns)
					}
					if fd.Global {
						// 避免后面添加到非全局规则里，此处直接清空
						filterMap = nil
					}
					result.Data.Detail.Filters = fd
				}
			}
		}
		newLoadedResults = append(newLoadedResults, result)

		if ok {
			for _, tp := range result.InvokerType.ToTypes() {
				var info *CrossDBInfo
				invoker := NewSQLInvokerPattern(result.InvokerName, tp)
				key := invoker.CreateKey()
				if info = newnewAllowCrossInfos[key]; info == nil {
					info = &CrossDBInfo{}
					newnewAllowCrossInfos[key] = info
				}

				if info.AllowAllDBs {
					// 所有库都允许，忽略其它条件
					continue
				}

				if result.AllowAllDBs {
					// 允许全部数据库跨库访问
					info.AllowAllDBs = true
					// 当 AllowAllDBs 为true时，不再需要 Groups 和 filters
					info.Groups = nil
					info.filters = nil
				} else {
					for _, filter := range filterMap {
						info.filters = append(info.filters, filter)
					}
					for _, gp := range result.Data.Detail.CrossDBGroups {
						info.Groups = append(info.Groups, CrossDBGroup{ID: result.ID, DBList: gp.DBList})
					}
				}
			}
		}
	}

	m.lck.Lock()
	defer m.lck.Unlock()

	m.allowCrossInfos = newnewAllowCrossInfos
	m.filters = newFilters
	m.loadedResults = newLoadedResults
	return nil
}

func (m *defaultCrossDBScheduler) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	// 什么也不做
	return nil
}

// CrossDBManager cross db manager
type CrossDBManager struct {
	schedulerWrapper[string, CrossDBInfo]
}

// Exclude method filter dbs
func (m *CrossDBManager) Exclude(dbNames []string) []string {
	failpoint.Inject("get-cross-db-excludes", func(val failpoint.Value) {
		patterns := strings.Split(val.(string), ",")
		if len(patterns) == 0 {
			panic(errors.New("filters is empty"))
		}

		var remain []string
		for _, pattern := range patterns {
			var (
				exclude mctech.Filter
				ok      bool
			)
			if exclude, ok = mctech.NewStringFilter(pattern); !ok {
				panic(errors.New("filter format is empty or error"))
			}
			for _, dbName := range dbNames {
				if !exclude.Match(dbName) {
					remain = append(remain, dbName)
				}
			}
		}
		failpoint.Return(remain)
	})

	if d, ok := m.Unwrap().(interface{ Exclude([]string) []string }); ok {
		return d.Exclude(dbNames)
	}
	return dbNames
}

// Get method inplements Scheduler interface
func (m *CrossDBManager) Get(pattern SQLInvokerPattern) *CrossDBInfo {
	failpoint.Inject("get-cross-db-info", func(val failpoint.Value) {
		var rules []map[string]any
		err := json.Unmarshal([]byte(val.(string)), &rules)
		if err != nil {
			panic(err)
		}
		for _, values := range rules {
			var (
				name string
				tp   InvokerType
			)
			info := &CrossDBInfo{}
			for k, v := range values {
				switch k {
				case "Service":
					name = v.(string)
					tp = InvokerTypeService
				case "Package":
					name = v.(string)
					tp = InvokerTypePackage
				case "AllowAllDBs":
					info.AllowAllDBs = v.(bool)
				case "Excludes":
					for _, item := range v.([]any) {
						db := item.(string)
						if filter, ok := mctech.NewStringFilter(db); ok {
							info.filters = append(info.filters, filter)
						}
					}
				case "Groups":
					for _, item := range v.([]any) {
						info.Groups = append(info.Groups, CrossDBGroup{
							ID:     int64(len(info.Groups)),
							DBList: strings.Split(item.(string), ","),
						})
					}
				}
			}

			_pattern := NewSQLInvokerPattern(name, tp)
			if _pattern.IsSame(pattern) {
				failpoint.Return(info)
			}
		}
		failpoint.Return(nil)
	})

	key := pattern.CreateKey()
	return m.Unwrap().Get(key)
}

// GetAll method inplements Scheduler interface
func (m *CrossDBManager) GetAll() map[string]*CrossDBInfo {
	return m.Unwrap().GetAll()
}

// GetLoadedResults get loaded results
func (m *CrossDBManager) GetLoadedResults() []*LoadedRuleResult {
	return m.Unwrap().(*defaultCrossDBScheduler).GetLoadedResults()
}

// NewCrossDBManager creates a new db cross manager
func NewCrossDBManager(sessPool sessionPool) *CrossDBManager {
	var scheduler schedulerWrapper[string, CrossDBInfo]
	ctx, cancel := context.WithCancel(context.Background())
	ctx = logutil.WithKeyValue(ctx, "allow-cross-db-worker", "allow-cross-db-manager")
	ctx = kv.WithInternalSourceType(ctx, "crossDBManager")
	scheduler = &defaultSchedulerWrapper[string, CrossDBInfo]{
		ctx:                   ctx,
		cancel:                cancel,
		sessPool:              sessPool,
		scheduleTicker:        time.NewTicker(crossDBManagerLoopTickerInterval),
		updateHeartBeatTicker: time.NewTicker(crossDBManagerLoopTickerInterval),
		worker: &defaultCrossDBScheduler{
			ctx:             ctx,
			allowCrossInfos: map[string]*CrossDBInfo{},
		},
	}
	return &CrossDBManager{scheduler}
}

func doFilterDbNames(filters []mctech.Filter, dbNames []string) []string {
	var remain []string
	for _, dbName := range dbNames {
		var matched bool
		for _, exclude := range filters {
			if matched = exclude.Match(dbName); matched {
				break
			}
		}
		if !matched {
			remain = append(remain, dbName)
		}
	}
	return remain
}

func parseDBGroupsAndFilters(r *LoadedRuleResult) (map[string]mctech.Filter, bool) {
	data := &r.Data
	groupList := config.StrToSlice(r.CrossDBs, "|")
	if len(groupList) == 0 {
		data.SetState(ResultStateTypeError, "Ignore. The 'cross_dbs' field is empty.")
		return nil, false
	}

	var (
		crossDBGroups []CrossDBGroupData
		filterMap     map[string]mctech.Filter
	)
	for index, group := range groupList {
		dbList := config.StrToSlice(group, ",")
		if len(dbList) <= 1 {
			data.SetState(ResultStateTypeError, fmt.Sprintf("Ignore. The number of databases in group(%d) is less than 2.", index))
			return nil, false
		}
		if slices.Contains(dbList, MatchAnyDB) {
			// 只要其中包含 [MatchAnyDB]，就认为该组中其它数据库全部不受跨库查询限制
			// 为了避免引起歧义，这种配置只允许出现在单一的配置规则中，不能在多个数据库分组规则中
			if len(groupList) > 1 {
				data.SetState(ResultStateTypeError, "Ignore. The '*' should be in single group, there are more than one db groups in this rule configuration.")
				return nil, false
			}

			for _, db := range dbList {
				if db == MatchAnyDB {
					continue
				}
				if filterMap == nil {
					filterMap = map[string]mctech.Filter{}
				}
				var (
					filter mctech.Filter
					ok     bool
				)
				if filter, ok = mctech.NewStringFilter(db); !ok {
					data.SetState(ResultStateTypeError, "Ignore. The db pattern is invalid.")
					return nil, false
				}
				filterMap[db] = filter
			}
		} else {
			crossDBGroups = append(crossDBGroups, CrossDBGroupData{DBList: dbList})
		}
	}

	data.SetState(ResultStateTypeSuccess, "Loaded Success")
	detail := r.initDetail()
	if !r.AllowAllDBs {
		detail.CrossDBGroups = crossDBGroups
	}
	return filterMap, true
}
