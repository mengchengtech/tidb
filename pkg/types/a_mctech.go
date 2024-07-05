// add by zhangbing

package types

// ParamMarkerOffset get ParamMarkerExpr offset
type ParamMarkerOffset interface {
	// 获取参数的顺序索引值
	GetOffset() int
}
