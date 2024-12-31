// add by zhangbing

package config

type MCTech struct {
	Sequence   Sequence   `toml:"sequence" json:"sequence"`
	Encryption Encryption `toml:"encryption" json:"encryption"`
	DbChecker  DbChecker  `toml:"db-checker" json:"db-checker"`
	Tenant     Tenant     `toml:"tenant" json:"tenant"`
}

type Sequence struct {
	ApiPrefix     string `toml:"api-prefix" json:"api-prefix"`
	Backend       int64  `toml:"backend" json:"backend"`
	Mock          bool   `toml:"mock" json:"mock"`
	Debug         bool   `toml:"debug" json:"debug"`
	MaxFetchCount int64  `toml:"max-fetch-count" json:"max-fetch-count"`
}

type DbChecker struct {
	Enabled          bool     `toml:"enabled" json:"enabled"`
	ApiPrefix        string   `toml:"api-prefix" json:"api-prefix"`
	MutexAcrossDbs   []string `toml:"mutex" json:"mutex"`
	ExcludeAcrossDbs []string `toml:"exclude" json:"exclude"`
	AcrossDbGroups   []string `toml:"across" json:"across"`
}

type Tenant struct {
	Enabled bool `toml:"enabled" json:"enabled"`
}

type Encryption struct {
	Mock      bool   `toml:"mock" json:"mock"`
	ApiPrefix string `toml:"api-prefix" json:"api-prefix"`
	AccessId  string `toml:"access-id" json:"access-id"`
}

func init() {
	defaultConf.MCTech = MCTech{
		Sequence: Sequence{
			Mock:          false,
			Debug:         false,
			MaxFetchCount: 1000,
			Backend:       3,
			ApiPrefix:     "http://node-infra-sequence-service.mc/",
		},
		Encryption: Encryption{
			Mock:      false,
			AccessId:  "oJEKJh1wvqncJYASxp1Iiw",
			ApiPrefix: "http://node-infra-encryption-service.mc/",
		},
		DbChecker: DbChecker{
			Enabled:          true,
			ApiPrefix:        "http://node-infra-dim-service.mc/",
			MutexAcrossDbs:   []string{},
			ExcludeAcrossDbs: []string{},
			AcrossDbGroups:   []string{},
		},
		Tenant: Tenant{
			Enabled: true,
		},
	}
}
