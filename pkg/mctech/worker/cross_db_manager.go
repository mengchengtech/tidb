package worker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	// CreateMCTechServiceCrossDB is a table about service cross db
	CreateMCTechServiceCrossDB = `CREATE TABLE IF NOT EXISTS mysql.mctech_service_cross_dbs (
		id bigint not null,
	  service varchar(64),
		cross_all_dbs boolean not null,
		cross_dbs varchar(1024) not null,
		enabled boolean not null,
		created_at datetime not null,
		loaded_at datetime,
		loading_result text,
    remark varchar(512),
		primary key (id)
	);`

	selectCrossDBTemplate    = "SELECT id, service, cross_all_dbs, cross_dbs from mysql.mctech_service_cross_dbs where enabled = true"
	updateLoadResultTemplate = "update mysql.mctech_service_cross_dbs set loaded_at = now(), loading_result = %%? where id in (%s)"
)

const crossDBManagerLoopTickerInterval = 30 * time.Second

// ServiceCrossDBInfo 允许特定服务跨库查询
type ServiceCrossDBInfo struct {
	Service  string
	AllowAll bool
	Groups   []CrossDBGroup
}

// CrossDBGroup 允许跨库查询的数据库组
type CrossDBGroup struct {
	ID int64
	// DBList 可以在同一个SQL中跨库查询的数据库列表
	DBList []string
}

type defaultCrossDBScheduler struct {
	ctx            context.Context
	allowCrossInfo map[string]*ServiceCrossDBInfo
}

// Get get ServiceCrossDBInfo
func (m *defaultCrossDBScheduler) Get(service string) *ServiceCrossDBInfo {
	failpoint.Inject("GetServiceCrossDBInfo", func(val failpoint.Value) {
		if val.(string) == service {
			at := &ServiceCrossDBInfo{
				Service:  service,
				AllowAll: false,
				Groups: []CrossDBGroup{
					{ID: -1, DBList: []string{"global_ipm", "global_mtlp"}},
				},
			}
			failpoint.Return(at)
		}
	})
	return m.allowCrossInfo[service]
}

func (m *defaultCrossDBScheduler) SetAll(all map[string]*ServiceCrossDBInfo) {
	m.allowCrossInfo = all
}

func (m *defaultCrossDBScheduler) GetAll() map[string]*ServiceCrossDBInfo {
	return m.allowCrossInfo
}

func (m *defaultCrossDBScheduler) ReloadAll(se sqlexec.SQLExecutor) error {
	rs, err := se.ExecuteInternal(m.ctx, selectCrossDBTemplate)
	if err != nil {
		return err
	}

	if rs == nil {
		return nil
	}

	defer func() {
		terror.Log(rs.Close())
	}()

	rows, err := sqlexec.DrainRecordSet(m.ctx, rs, 8)
	if err != nil {
		return err
	}

	newMap := map[string]*ServiceCrossDBInfo{}
	okList := make([]string, 0, len(rows))
	for _, row := range rows {
		id := row.GetInt64(0)
		service := row.GetString(1)
		allAll := row.GetInt64(2) != 0
		crossDBList := config.StrToSlice(row.GetString(3), ",")

		if !allAll && len(crossDBList) <= 1 {
			// update warn
			msg := "Ignore. The number of databases is less than 2"
			sql := fmt.Sprintf(updateLoadResultTemplate, strconv.FormatInt(id, 10))
			if _, err := se.ExecuteInternal(m.ctx, sql, msg); err != nil {
				return err
			}
		} else {
			okList = append(okList, strconv.FormatInt(id, 10))

			var info *ServiceCrossDBInfo
			if info = newMap[service]; info == nil {
				info = &ServiceCrossDBInfo{
					Service: service,
					Groups:  []CrossDBGroup{},
				}
				newMap[service] = info
			}
			if allAll {
				// 允许全部数据库跨库访问
				info.AllowAll = true
			}
			info.Groups = append(info.Groups, CrossDBGroup{ID: id, DBList: crossDBList})
		}
	}

	m.allowCrossInfo = newMap
	if len(okList) > 0 {
		args := []any{"Success.", okList}
		sql := fmt.Sprintf(updateLoadResultTemplate, strings.Join(okList, ","))
		if _, err := se.ExecuteInternal(m.ctx, sql, args...); err != nil {
			return err
		}
	}

	return nil
}

func (m *defaultCrossDBScheduler) UpdateHeartBeat(ctx context.Context, se sqlexec.SQLExecutor) error {
	// 什么也不做
	return nil
}

// CrossDBManager service cross db manager
type CrossDBManager struct {
	schedulerWrapper[ServiceCrossDBInfo]
}

// Get method inplements Scheduler interface
func (m *CrossDBManager) Get(service string) *ServiceCrossDBInfo {
	s := m.unwrap()
	if s != nil {
		return s.(*defaultCrossDBScheduler).Get(service)
	}
	return nil
}

// NewCrossDBManager creates a new db cross manager
func NewCrossDBManager(sessPool sessionPool) *CrossDBManager {
	var scheduler schedulerWrapper[ServiceCrossDBInfo]
	ctx, cancel := context.WithCancel(context.Background())
	ctx = logutil.WithKeyValue(ctx, "allow-cross-db-worker", "allow-cross-db-manager")
	ctx = kv.WithInternalSourceType(ctx, "crossDBManager")
	scheduler = &defaultSchedulerWrapper[ServiceCrossDBInfo]{
		ctx:                   ctx,
		cancel:                cancel,
		sessPool:              sessPool,
		scheduleTicker:        time.NewTicker(crossDBManagerLoopTickerInterval),
		updateHeartBeatTicker: time.NewTicker(crossDBManagerLoopTickerInterval),
		worker: &defaultCrossDBScheduler{
			ctx:            ctx,
			allowCrossInfo: map[string]*ServiceCrossDBInfo{},
		},
	}
	return &CrossDBManager{scheduler}
}
