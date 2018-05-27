// +build go1.6

package testing

import (
	"crypto/rand"
)

func GetRawExample() []TestValue {
	return []TestValue{
		&RawTest{"ID_RAW_1", genRand(1024)},
		&RawTest{"ID_RAW_2", genRand(512)},
	}
}

func genRand(size int) []byte {
	buf := make([]byte, size)
	rand.Read(buf)
	return buf
}
