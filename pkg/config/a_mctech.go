// add by zhangbing

package config

type MCTech struct {
	Sequence   Sequence   `toml:"sequence" json:"sequence"`
	Encryption Encryption `toml:"encryption" json:"encryption"`
}

type Sequence struct {
	ApiPrefix     string `toml:"api-prefix" json:"api-prefix"`
	Backend       int64  `toml:"backend" json:"backend"`
	Mock          bool   `toml:"mock" json:"mock"`
	Debug         bool   `toml:"debug" json:"debug"`
	MaxFetchCount int64  `toml:"max-fetch-count" json:"max-fetch-count"`
}

type Encryption struct {
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
			AccessId:  "oJEKJh1wvqncJYASxp1Iiw",
			ApiPrefix: "http://node-infra-encryption-service.mc/",
		},
	}
}
