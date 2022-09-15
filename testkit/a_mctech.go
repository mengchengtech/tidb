package testkit

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
)

// Exec executes a sql statement using the prepared stmt API with Context
func (tk *TestKit) MustExecWithContext(
	ctx context.Context, sql string, args ...interface{}) {
	res, err := tk.ExecWithContext(ctx, sql, args...)
	comment := fmt.Sprintf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err))
	tk.require.NoError(err, comment)

	if res != nil {
		tk.require.NoError(res.Close())
	}
}

// Exec executes a sql statement using the prepared stmt API with Context
func (tk *TestKit) MustQueryWithContext(
	ctx context.Context, sql string, args ...interface{}) *Result {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	rs, err := tk.ExecWithContext(ctx, sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(rs, comment)
	return tk.ResultSetToResult(rs, comment)
}
