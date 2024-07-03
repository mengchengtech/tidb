// add by zhangbing

package infoschema

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

const (
	// TableMCTechLargeQuery is the string constant of large sql query memory table.
	TableMCTechLargeQuery = "MCTECH_LARGE_QUERY"

	// ClusterTableMCTechLargeQuery is the string constant of cluster large sql query memory table.
	ClusterTableMCTechLargeQuery = "CLUSTER_MCTECH_LARGE_QUERY"

	// TableMCTechTableTTLInfo is the string constant of mctech table ttl info
	TableMCTechTableTTLInfo = "MCTECH_TABLE_TTL_INFO"
)

var mctechLargeQueryCols = []columnInfo{
	{name: variable.MCLargeQueryTimeStr, tp: mysql.TypeTimestamp, size: 26, decimal: 6, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.BinaryFlag},
	{name: variable.MCLargeQueryUserStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeQueryHostStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeQueryDBStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeQueryDigestStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeQuerySQLLengthStr, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCLargeQueryServiceStr, tp: mysql.TypeVarchar, size: 128},
	{name: variable.MCLargeQuerySQLTypeStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeQuerySQLStr, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.MCLargeQuerySuccStr, tp: mysql.TypeTiny, size: 1},
	{name: variable.MCLargeQueryMemMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCLargeQueryDiskMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCLargeQueryResultRows, tp: mysql.TypeLonglong, size: 22},
	{name: variable.MCLargeQueryPlan, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},

	{name: variable.MCLargeQueryQueryTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeQueryParseTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeQueryCompileTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeQueryRewriteTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeQueryOptimizeTimeStr, tp: mysql.TypeDouble, size: 22},

	{name: execdetails.MCLargeQueryWriteKeysStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.MCLargeQueryWriteSizeStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.MCLargeQueryCopTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCLargeQueryProcessTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCLargeQueryWaitTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCLargeQueryTotalKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
}

var mcTechTableTTLInfoCols = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "TTL_COLUMN_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TTL_COLUMN_TYPE", tp: mysql.TypeBlob, size: 196606},
	{name: "TTL_COLUMN_GENERATED_EXPR", tp: mysql.TypeVarchar, size: 512},
	{name: "TTL", tp: mysql.TypeVarchar, size: 64},
	{name: "TTL_UNIT", tp: mysql.TypeVarchar, size: 64},
	{name: "TTL_ENABLE", tp: mysql.TypeVarchar, size: 21},
	{name: "TTL_JOB_INTERVAL", tp: mysql.TypeVarchar, size: 64},
}

func init() {
	const mctechInformationSchemaDBID = 100000000
	memTableToAllTiDBClusterTables[TableMCTechLargeQuery] = ClusterTableMCTechLargeQuery

	tableIDMap[TableMCTechLargeQuery] = mctechInformationSchemaDBID + 1
	tableIDMap[ClusterTableMCTechLargeQuery] = mctechInformationSchemaDBID + 2
	tableIDMap[TableMCTechTableTTLInfo] = mctechInformationSchemaDBID + 3

	tableNameToColumns[TableMCTechLargeQuery] = mctechLargeQueryCols
	tableNameToColumns[TableMCTechTableTTLInfo] = mcTechTableTTLInfoCols
}
