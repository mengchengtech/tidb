// add by zhangbing

package infoschema

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
)

const (
	// TableMCTechLargeQuery is the string constant of large sql query memory table.
	TableMCTechLargeQuery = "MCTECH_LARGE_QUERY"

	// ClusterTableMCTechLargeQuery is the string constant of cluster large sql query memory table.
	ClusterTableMCTechLargeQuery = "CLUSTER_MCTECH_LARGE_QUERY"
)

var mctechLargeQueryCols = []columnInfo{
	{name: variable.MCTechLargeQueryTimeStr, tp: mysql.TypeTimestamp, size: 26, decimal: 6, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.BinaryFlag},
	{name: variable.MCTechLargeQueryUserStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCTechLargeQueryHostStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCTechLargeQueryDBStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCTechLargeQueryDigestStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCTechLargeQuerySQLLengthStr, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCTechLargeQueryServiceStr, tp: mysql.TypeVarchar, size: 128},
	{name: variable.MCTechLargeQuerySQLTypeStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCTechLargeQuerySQLStr, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.MCTechLargeQuerySuccStr, tp: mysql.TypeTiny, size: 1},
	{name: variable.MCTechLargeQueryMemMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCTechLargeQueryDiskMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCTechLargeQueryResultRows, tp: mysql.TypeLonglong, size: 22},
	{name: variable.MCTechLargeQueryPlan, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},

	{name: variable.MCTechLargeQueryQueryTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCTechLargeQueryParseTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCTechLargeQueryCompileTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCTechLargeQueryRewriteTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCTechLargeQueryOptimizeTimeStr, tp: mysql.TypeDouble, size: 22},

	{name: execdetails.MCTechWriteKeysStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.MCTechWriteSizeStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.MCTechCopTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCTechProcessTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCTechWaitTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCTechTotalKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
}

func init() {
	const mctechInformationSchemaDBID = 100000000
	memTableToAllTiDBClusterTables[TableMCTechLargeQuery] = ClusterTableMCTechLargeQuery

	tableIDMap[TableMCTechLargeQuery] = mctechInformationSchemaDBID + 1
	tableIDMap[ClusterTableMCTechLargeQuery] = mctechInformationSchemaDBID + 2

	tableNameToColumns[TableMCTechLargeQuery] = mctechLargeQueryCols
}
