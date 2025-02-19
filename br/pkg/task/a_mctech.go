// add by zhangbing

package task

import "github.com/spf13/pflag"

const (
	// flagSkipTiFlashRestore represents whether skip tiflash replica restore if tiflash exists
	flagSkipTiFlashRestore = "mctech.skip-tiflash-restore"

	// flagForceReplicaCount represents set tiflash replica restore count if tiflash exists
	flagForceReplicaCount = "mctech.force-replica-count"

	// flagIgnorePlacement represents whether ignore db and table placements
	flagIgnorePlacement = "mctech.ignore-placement"

	// flagRestoreTableSuffix represents restore table name suffix
	flagRestoreTableSuffix = "mctech.restore-table-suffix"

	// flagRestoreDBSuffix represents restore database name suffix
	flagRestoreDBSuffix = "mctech.restore-db-suffix"
)

type MCTechRestoreConfig struct {
	SkipTiFlashRestore bool   `json:"skip-tiflash-restore" toml:"skip-tiflash-restore"`
	ForceReplicaCount  uint16 `json:"force-replica-count" toml:"force-replica-count"`
	IgnorePlacement    bool   `json:"ignore-placement" toml:"ignore-placement"`
	RestoreTableSuffix string `json:"restore-table-suffix" toml:"restore-table-suffix"`
	RestoreDBSuffix    string `json:"restore-db-suffix" toml:"restore-db-suffix"`
}

func defineMCTechRestoreFlags(flags *pflag.FlagSet) {
	flags.Bool(flagSkipTiFlashRestore, false, "whether skip tiflash replica restore if tiflash exists (default false)")
	flags.Uint16(flagForceReplicaCount, 0, "change tiflash replica count if tiflash exists, use restore meta information. no action when set 0 (default 0)")
	flags.Bool(flagIgnorePlacement, false, "whether ignore db and table placements (default false)")
	flags.String(flagRestoreTableSuffix, "", "restore table name suffix (default \"\")")
	flags.String(flagRestoreDBSuffix, "", "restore db name suffix (default \"\")")
}
