// add by zhangbing

package server

func appendUint32(b []byte, v uint32) []byte {
	return append(b,
		byte(v),
		byte(v>>8),
		byte(v>>16),
		byte(v>>24),
	)
}
