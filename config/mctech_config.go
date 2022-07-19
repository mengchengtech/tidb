package config

// MCTech mctech custom config
type MCTech struct {
	Sequence   Sequence   `toml:"sequence" json:"sequence"`
	Encryption Encryption `toml:"encryption" json:"encryption"`
	DbChecker  DbChecker  `toml:"db-checker" json:"db-checker"`
	Tenant     Tenant     `toml:"tenant" json:"tenant"`
	DDL        DDL        `toml:"ddl" json:"ddl"`
}

// Sequence mctech_sequence functions used
type Sequence struct {
	APIPrefix     string `toml:"api-prefix" json:"api-prefix"`
	Backend       int64  `toml:"backend" json:"backend"`
	Mock          bool   `toml:"mock" json:"mock"`
	Debug         bool   `toml:"debug" json:"debug"`
	MaxFetchCount int64  `toml:"max-fetch-count" json:"max-fetch-count"`
}

// DbChecker db isolation check used
type DbChecker struct {
	Enabled          bool     `toml:"enabled" json:"enabled"`
	APIPrefix        string   `toml:"api-prefix" json:"api-prefix"`
	MutexAcrossDbs   []string `toml:"mutex" json:"mutex"`
	ExcludeAcrossDbs []string `toml:"exclude" json:"exclude"`
	AcrossDbGroups   []string `toml:"across" json:"across"`
}

// Tenant append tenant condition used
type Tenant struct {
	Enabled          bool `toml:"enabled" json:"enabled"`
	ForbiddenPrepare bool `toml:"forbidden-prepare" json:"forbidden-prepare"`
}

// Encryption custom crypto function used
type Encryption struct {
	Mock      bool   `toml:"mock" json:"mock"`
	APIPrefix string `toml:"api-prefix" json:"api-prefix"`
	AccessID  string `toml:"access-id" json:"access-id"`
}

// DDL custom ddl config
type DDL struct {
	Version VersionColumn `toml:"version" json:"version"`
}

// VersionColumn auto add version column
type VersionColumn struct {
	Enabled   bool     `toml:"enabled" json:"enabled"`
	Name      string   `toml:"name" json:"name"`
	DbMatches []string `toml:"db-matches" json:"db-matches"`
}
