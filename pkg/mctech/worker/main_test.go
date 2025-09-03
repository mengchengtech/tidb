package worker

import (
	"github.com/pingcap/tidb/pkg/mctech"
)

var (
	_ modifyWorkerScheduler[string, CrossDBInfo]    = &defaultCrossDBScheduler{}
	_ modifyWorkerScheduler[string, DenyDigestInfo] = &defaultDigestScheduler{}
	_ modifyWorkerScheduler[string, any]            = &nonWorkerScheduler[string, any]{}
)

type modifyWorkerScheduler[TKey, TValue any] interface {
	SetAll(all map[string]*TValue)
}

func NewCrossDBInfo(allowAllDBs bool, patterns []string, groups []CrossDBGroup) *CrossDBInfo {
	var filters []mctech.Filter
	for _, filter := range patterns {
		if exclude, ok := mctech.NewStringFilter(filter); ok {
			filters = append(filters, exclude)
		}
	}

	return &CrossDBInfo{
		AllowAllDBs: allowAllDBs,
		filters:     filters,
		Groups:      groups,
	}
}

func CreateCrossDBDetail(crossDBGroups [][]string, filters *FilterData, name string, tp InvokerType, allowAllDBs bool) *CrossDBDetailData {
	var groupDatas []CrossDBGroupData
	if len(crossDBGroups) > 0 {
		groupDatas = make([]CrossDBGroupData, 0, len(crossDBGroups))
		for _, dbList := range crossDBGroups {
			groupDatas = append(groupDatas, CrossDBGroupData{DBList: dbList})
		}
	}
	detail := &CrossDBDetailData{AllowAllDBs: allowAllDBs, CrossDBGroups: groupDatas, Filters: filters}
	detail.init(name, tp)
	return detail
}

// SetAll returns the deny digests
func (m *DigestManager) SetAll(denyDigests map[string]*DenyDigestInfo) {
	m.Unwrap().(modifyWorkerScheduler[string, DenyDigestInfo]).SetAll(denyDigests)
}

// SetAll returns the deny digests
func (m *CrossDBManager) SetAll(cross map[string]*CrossDBInfo) {
	m.Unwrap().(modifyWorkerScheduler[string, CrossDBInfo]).SetAll(cross)
}
