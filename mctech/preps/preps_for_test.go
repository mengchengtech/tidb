package preps

import "github.com/pingcap/tidb/mctech"

func NewMutuallyExclusiveDatabaseCheckerWithParamsForTest(mutex, exclude, across []string) *mutuallyExclusiveDatabaseChecker {
	return newMutuallyExclusiveDatabaseCheckerWithParams(mutex, exclude, across)
}

func GetDWSelectorForTest() mctech.DWSelector {
	return newDWSelector()
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
	Parse(mctx mctech.Context,
		actions []ActionInfo, comments mctech.Comments, params map[string]any) (mctech.ParseResult, error)
	GetParsedSQL() string
}

type sqlPreprocessorForTest struct {
	sqlPreprocessor
}

func (p *sqlPreprocessorForTest) GetParsedSQL() string {
	return p.sql
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
