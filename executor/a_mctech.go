package executor

import (
	"context"

	"github.com/pingcap/tidb/parser/ast"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
)

func init() {
	// mctech functions
	funcName2Alias["mctech_sequence"] = ast.MCTechSequence
	funcName2Alias["mctech_version_just_pass"] = ast.MCTechVersionJustPass
	funcName2Alias["mctech_decrypt"] = ast.MCTechDecrypt
	funcName2Alias["mctech_encrypt"] = ast.MCTechEncrypt
	// add end
}

// buildMCTech builds a explain executor. `e.rows` collects final result to `MCTechExec`.
func (b *executorBuilder) buildMCTech(v *plannercore.MCTech) Executor {
	explainExec := &MCTechExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ID()),
		mctech:       v,
	}
	return explainExec
}

// MCTechExec represents an explain executor.
type MCTechExec struct {
	baseExecutor

	// 对应的执行计划类的实例
	mctech *plannercore.MCTech
	// 需要返回的行列数据
	rows   [][]*types.Datum
	cursor int
}

// Open implements the Executor Open interface.
func (e *MCTechExec) Open(ctx context.Context) error {
	// MCTECH语句不需要做额外的事情
	return nil
}

// Close implements the Executor Close interface.
func (e *MCTechExec) Close() error {
	// MCTECH语句不需要做额外的事情，只需要简单的把返回结果清除下
	e.rows = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *MCTechExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.rows == nil {
		if err := e.mctech.RenderResult(); err != nil {
			return err
		}
		e.rows = e.mctech.Rows
	}

	// 一定要执行这行代码，初始化返回结果集存储空间，否则程序会卡死
	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.rows) {
		return nil
	}

	numCurRows := mathutil.Min(req.Capacity(), len(e.rows)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurRows; i++ {
		// 根据schema里定义的类型调用不同的方法
		for j := range e.rows[i] {
			datum := e.rows[i][j]
			// log.Info(fmt.Sprintf("%d -> kind: %d", j, datum.Kind()))
			req.AppendDatum(j, datum)
		}
	}
	e.cursor += numCurRows
	return nil
}
