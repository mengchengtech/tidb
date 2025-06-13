package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
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
		loaded_at datetime,
		loaded_result json,
		primary key (id)
	);`

	updateLoadResultSQL = "update %n.%n set loaded_at = now(), loaded_result = %? where id = %?"
	selectCrossDBSQL    = "SELECT id, invoker_name, invoker_type, allow_all_dbs, cross_dbs, enabled, loaded_result from %n.%n"
)

// InvokerType invoker type. such as 'service' or 'package'
type InvokerType string

const (
	// InvokerTypeService 代表给定的Invoker是服务类型
	InvokerTypeService InvokerType = "service"
	// InvokerTypePackage 代表给定的Invoker是依赖包类型
	InvokerTypePackage InvokerType = "package"
	// InvokerTypeBoth 代表给定的Invoker既可以是服务类型也可以是依赖包类型
	InvokerTypeBoth InvokerType = "both"
)

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

const (
	// MatchAnyInvoker define string that match any invoker name
	MatchAnyInvoker = "*"
	// MatchAnyDB define string that match any db name
	MatchAnyDB = "*"
)

const crossDBManagerLoopTickerInterval = 30 * time.Second

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
	return fmt.Sprintf("%s@%s", s.name, s.tp)
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
}

// CrossDBGroup 允许跨库查询的数据库组
type CrossDBGroup struct {
	ID int64
	// DBList 可以在同一个SQL中跨库查询的数据库列表
	DBList []string
}

type defaultCrossDBScheduler struct {
	ctx            context.Context
	lck            sync.RWMutex
	allowCrossInfo map[string]*CrossDBInfo
}

type loadedResult struct {
	State   string
	Message string
	Data    *crossDBData
}

func (r *loadedResult) Equal(other *loadedResult) bool {
	if r == nil && other == nil {
		return true
	}
	if r != nil && other == nil || r == nil && other != nil {
		return false
	}

	if r.State != other.State || r.Message != other.Message {
		return false
	}
	if r.Data == nil && other.Data == nil {
		return true
	}
	if r.Data != nil && other.Data != nil {
		return false
	} else if r.Data == nil && other.Data != nil {
		return false
	}
	// c.Data != nil && c.Data != nil
	return r.Data.Equal(other.Data)
}

func (r *loadedResult) MarshalJSON() ([]byte, error) {
	arr := []any{r.State, r.Message}
	if r.Data != nil {
		arr = append(arr, r.Data)
	}
	return json.Marshal(arr)
}

func (r *loadedResult) UnmarshalJSON(data []byte) (err error) {
	var arr []any
	if err = json.Unmarshal(data, &arr); err != nil {
		return err
	}

	var cd crossDBData
	switch {
	case len(arr) == 3:
		m := arr[2].(map[string]any)
		if m != nil {
			var bytes []byte
			if bytes, err = json.Marshal(m); err != nil {
				return err
			}

			if err = json.Unmarshal(bytes, &r.Data); err != nil {
				return err
			}
		} else {
			return &json.InvalidUnmarshalError{Type: reflect.TypeOf(cd)}
		}
		fallthrough
	case len(arr) == 2:
		r.State = arr[0].(string)
		r.Message = arr[1].(string)
	default:
		return &json.InvalidUnmarshalError{Type: reflect.TypeOf(r)}
	}
	return nil
}

type crossDBData struct {
	Service     string   `json:"service,omitempty"`
	Package     string   `json:"package,omitempty"`
	AllowAllDBs *bool    `json:"allow-all-dbs,omitempty"`
	CrossDBs    []string `json:"cross-dbs,omitempty"`
}

func (c *crossDBData) Equal(other *crossDBData) bool {
	if c == nil && other == nil {
		return true
	}
	if c != nil && other == nil || c == nil && other != nil {
		return false
	}

	var a bool
	var b bool
	if c.AllowAllDBs != nil {
		a = *c.AllowAllDBs
	}
	if other.AllowAllDBs != nil {
		b = *other.AllowAllDBs
	}
	return c.Service == other.Service &&
		c.Package == other.Package &&
		a == b &&
		slices.Compare(c.CrossDBs, other.CrossDBs) == 0
}

func newCrossDBData(name string, tp InvokerType) *crossDBData {
	data := &crossDBData{}
	switch tp {
	case InvokerTypeService:
		data.Service = name
	case InvokerTypePackage:
		data.Package = name
	case InvokerTypeBoth:
		data.Package = name
		data.Service = name
	}
	return data
}

// Get get CrossDBInfo
func (m *defaultCrossDBScheduler) Get(key string) *CrossDBInfo {
	m.lck.RLock()
	defer m.lck.RUnlock()

	if key == "" {
		key = MatchAnyInvoker
	}
	return m.allowCrossInfo[key]
}

func (m *defaultCrossDBScheduler) SetAll(all map[string]*CrossDBInfo) {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.allowCrossInfo = all
}

func (m *defaultCrossDBScheduler) GetAll() map[string]*CrossDBInfo {
	m.lck.RLock()
	defer m.lck.RUnlock()

	if m.allowCrossInfo == nil {
		return nil
	}
	return maps.Clone(m.allowCrossInfo)
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

	newMap := map[string]*CrossDBInfo{}
	for _, row := range rows {
		id := row.GetInt64(0)
		invokerName := row.GetString(1)
		invokerType := InvokerType(row.GetEnum(2).Name)
		allowAllDBs := row.GetInt64(3) != 0
		crossDBList := config.StrToSlice(row.GetString(4), ",")
		enabled := row.GetInt64(5) != 0
		var oldResult *loadedResult
		if !row.IsNull(6) {
			r := loadedResult{}
			var d []byte
			if d, err = row.GetJSON(6).MarshalJSON(); err != nil {
				return err
			}
			if err = json.Unmarshal(d, &r); err == nil {
				oldResult = &r
			}
		}

		var newResult *loadedResult
		switch {
		case !enabled:
			// update disabled
			newResult = &loadedResult{State: "disabled", Message: "current rule is Disabled"}
		case len(invokerName) == 0:
			// update error
			newResult = &loadedResult{State: "error", Message: "Ignore. The invoker_name field must not be empty."}
		case !allowAllDBs && len(crossDBList) <= 1:
			// update error
			newResult = &loadedResult{State: "error", Message: "Ignore. The number of databases is less than 2"}
		default:
			data := newCrossDBData(invokerName, invokerType)
			// update ok
			newResult = &loadedResult{State: "success", Message: "Loaded Success", Data: data}
			if allowAllDBs {
				data.AllowAllDBs = &allowAllDBs
			} else {
				data.CrossDBs = crossDBList
			}

			for _, tp := range invokerType.ToTypes() {
				var info *CrossDBInfo
				invoker := NewSQLInvokerPattern(invokerName, tp)
				key := invoker.CreateKey()
				if info = newMap[key]; info == nil {
					info = &CrossDBInfo{}
					newMap[key] = info
				}

				if allowAllDBs {
					// 允许全部数据库跨库访问
					info.AllowAllDBs = true
				} else {
					info.Groups = append(info.Groups, CrossDBGroup{ID: id, DBList: crossDBList})
				}
			}
		}

		if newResult.Equal(oldResult) {
			// 状态相同，不需要更新
			continue
		}
		newBytes, _ := json.Marshal(newResult)
		sql := updateLoadResultSQL
		args := []any{
			mysql.SystemDB, MCTechCrossDB,
			string(newBytes), id,
		}
		if _, err := se.ExecuteInternal(m.ctx, sql, args...); err != nil {
			// 忽略返回的异常信息，只记录异常日志
			return fmt.Errorf("update loaded result error. sql: %s. %w", sql, err)
		}
	}

	m.lck.Lock()
	defer m.lck.Unlock()

	m.allowCrossInfo = newMap
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
			ctx:            ctx,
			allowCrossInfo: map[string]*CrossDBInfo{},
		},
	}
	return &CrossDBManager{scheduler}
}
