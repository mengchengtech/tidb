package preps

import "github.com/pingcap/tidb/pkg/mctech"

func NewMutexDatabaseCheckerWithParamsForTest(mutex, exclude, across []string) *mutexDatabaseChecker {
	return newMutexDatabaseCheckerWithParams(mutex, exclude, across)
}

func NewDBSelectorForTest(result mctech.PrepareResult) mctech.DBSelector {
	return newDBSelector(result)
}

func NewGlobalValueFormatterForTest() valueFormatter {
	return newGlobalValueFormatter()
}

func GetPreprocessorForTest() StatementPreprocessor {
	return preprocessor
}

func GetDatabaseCheckerForTest() DatabaseChecker {
	return getDatabaseChecker()
}

func NewActionsForTest(m map[string]string) (result []ActionInfo) {
	for name, args := range m {
		info := &actionInfo{name: name, args: args}
		result = append(result, info)
	}
	return result
}

type SQLPreprocessorTest interface {
	Prepare(mctx mctech.Context,
		actions []ActionInfo, comments mctech.Comments, params map[string]any) (mctech.PrepareResult, error)
	GetPreparedSQL() string
}

type sqlPreprocessorForTest struct {
	sqlPreprocessor
}

func (p *sqlPreprocessorForTest) GetPreparedSQL() string {
	return p.preparedSQL
}

func NewSQLPreprocessorForTest(sql string) SQLPreprocessorTest {
	return &sqlPreprocessorForTest{
		sqlPreprocessor: *newSQLPreprocessor(sql),
	}
}

type ActionForTest interface {
	action
}

func NewReplaceActionForTest() ActionForTest {
	return &replaceAction{}
}
