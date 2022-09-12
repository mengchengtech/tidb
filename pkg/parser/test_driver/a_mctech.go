package test_driver

// GetOffset implements the ParamMarkerExpr interface.
func (n *ParamMarkerExpr) GetOffset() int {
	return n.Offset
}
