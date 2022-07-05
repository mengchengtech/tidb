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
