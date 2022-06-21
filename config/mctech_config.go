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
	Mock      bool   `toml:"mock" json:"mock"`
	ApiPrefix string `toml:"api-prefix" json:"api-prefix"`
	AccessId  string `toml:"access-id" json:"access-id"`
}
