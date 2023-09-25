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
)

var mctechLargeQueryCols = []columnInfo{
	{name: variable.MCLargeLogTimeStr, tp: mysql.TypeTimestamp, size: 26, decimal: 6, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.BinaryFlag},
	{name: variable.MCLargeLogUserStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeLogHostStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeLogDBStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeLogDigestStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeLogSQLLengthStr, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCLargeLogServiceStr, tp: mysql.TypeVarchar, size: 128},
	{name: variable.MCLargeLogSQLTypeStr, tp: mysql.TypeVarchar, size: 64},
	{name: variable.MCLargeLogSQLStr, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: variable.MCLargeLogSuccStr, tp: mysql.TypeTiny, size: 1},
	{name: variable.MCLargeLogMemMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCLargeLogDiskMax, tp: mysql.TypeLonglong, size: 20},
	{name: variable.MCLargeLogResultRows, tp: mysql.TypeLonglong, size: 22},
	{name: variable.MCLargeLogPlan, tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},

	{name: variable.MCLargeLogQueryTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeLogParseTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeLogCompileTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeLogRewriteTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: variable.MCLargeLogOptimizeTimeStr, tp: mysql.TypeDouble, size: 22},

	{name: execdetails.MCLargeLogWriteKeysStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.MCLargeLogWriteSizeStr, tp: mysql.TypeLonglong, size: 22},
	{name: execdetails.MCLargeLogCopTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCLargeLogProcessTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCLargeLogWaitTimeStr, tp: mysql.TypeDouble, size: 22},
	{name: execdetails.MCLargeLogTotalKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.UnsignedFlag},
}

func init() {
	const mctechInformationSchemaDBID = 100000000
	memTableToAllTiDBClusterTables[TableMCTechLargeQuery] = ClusterTableMCTechLargeQuery

	tableIDMap[TableMCTechLargeQuery] = mctechInformationSchemaDBID + 1
	tableIDMap[ClusterTableMCTechLargeQuery] = mctechInformationSchemaDBID + 2

	tableNameToColumns[TableMCTechLargeQuery] = mctechLargeQueryCols
}
