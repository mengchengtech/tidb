package preps

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommentFromPattern(t *testing.T) {
	sql := `/* from:'node-pf-wechat-rag-bot-service.pf' , addr:'10.180.102.220' */ select * from t1`
	comments := GetCustomCommentFromSQL(sql)
	require.Equal(t, "node-pf-wechat-rag-bot-service", comments.Service().AppName())
	require.Equal(t, "pf", comments.Service().ProductLine())
	require.Equal(t, "node-pf-wechat-rag-bot-service.pf", comments.Service().From())
}

func TestCommentPackagePattern(t *testing.T) {
	sql := `/* package:'@mctech/dp-impala-tidb-enhanced', this is constraint check sql */ select * from t1`
	comments := GetCustomCommentFromSQL(sql)
	require.Equal(t, "@mctech/dp-impala-tidb-enhanced", comments.Package().Name())
}

func TestCommentFromAndPackagePattern(t *testing.T) {
	sql := `/* from: 'node-pf-wechat-rag-bot-service.pf', addr:'10.180.102.220' */ 
/* package:'@mctech/dp-impala-tidb-enhanced', this is constraint check sql */
select * from t1`
	comments := GetCustomCommentFromSQL(sql)
	require.Equal(t, "node-pf-wechat-rag-bot-service", comments.Service().AppName())
	require.Equal(t, "pf", comments.Service().ProductLine())
	require.Equal(t, "node-pf-wechat-rag-bot-service.pf", comments.Service().From())

	require.Equal(t, "@mctech/dp-impala-tidb-enhanced", comments.Package().Name())
}
