package testdata

import (
	"encoding/binary"
	"fmt"
)

func KeyInt(i int) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

func ValueInt(i int) []byte {
	return []byte(fmt.Sprintf("value-for-%d", i))
}
