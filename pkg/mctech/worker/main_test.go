package worker

var (
	_ modifyWorkerScheduler[string, CrossDBInfo]    = &defaultCrossDBScheduler{}
	_ modifyWorkerScheduler[string, DenyDigestInfo] = &defaultDigestScheduler{}
	_ modifyWorkerScheduler[string, any]            = &nonWorkerScheduler[string, any]{}
)

type modifyWorkerScheduler[TKey, TValue any] interface {
	SetAll(all map[string]*TValue)
}

func CreateCrossDBDetail(crossDBGroups [][]string, name string, tp InvokerType, allowAllDBs bool) *CrossDBDetailData {
	var groupDatas []CrossDBGroupData
	if len(crossDBGroups) > 0 {
		groupDatas = make([]CrossDBGroupData, 0, len(crossDBGroups))
		for _, dbList := range crossDBGroups {
			groupDatas = append(groupDatas, CrossDBGroupData{DBList: dbList})
		}
	}
	detail := &CrossDBDetailData{AllowAllDBs: allowAllDBs, CrossDBGroups: groupDatas}
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
