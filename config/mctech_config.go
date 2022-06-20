package config

type MCTech struct {
	Sequence   Sequence   `toml:"sequence" json:"sequence"`
	Encryption Encryption `toml:"encryption" json:"encryption"`
}

type Sequence struct {
	ApiPrefix string `toml:"api-prefix" json:"api-prefix"`
	Backend   int64  `toml:"backend" json:"backend"`
}

type Encryption struct {
	ApiPrefix string `toml:"api-prefix" json:"api-prefix"`
	AccessId  string `toml:"access-id" json:"access-id"`
}
