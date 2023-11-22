// add by zhangbing
package task

const (
	// FlagSkipTiFlashRestore represents whether skip tiflash replica restore if tiflash exists
	FlagSkipTiFlashRestore = "skip-tiflash-restore"

	// FlagForceReplicaCount represents set tiflash replica restore count if tiflash exists
	FlagForceReplicaCount = "force-replica-count"

	// FlagIgnorePlacement represents whether ignore db and table placements
	FlagIgnorePlacement = "ignore-placement"

	// FlagRestoreTableSuffix represents restore table name suffix
	FlagRestoreTableSuffix = "restore-table-suffix"

	// FlagRestoreDBSuffix represents restore database name suffix
	FlagRestoreDBSuffix = "restore-db-suffix"
)
