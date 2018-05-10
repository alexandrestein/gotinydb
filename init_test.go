package db

import (
	"crypto/rand"
	"os"
	"time"
)

var (
	path = os.TempDir() + "/dbTest"
)

type (
	UserTest struct {
		ID, UserName, Password string
		Creation               time.Time
	}

	RawTest struct {
		ID      string
		Content []byte
	}
)

func getUsersExample() []*UserTest {
	return []*UserTest{
		&UserTest{"ID_USER_1", "mister 1", "pass 1", time.Now()},
		&UserTest{"ID_USER_2", "mister 2", "pass 2", time.Now().Add(time.Hour * 3600)},
	}
}

func getRawExample() []*RawTest {
	return []*RawTest{
		&RawTest{"ID_RAW_1", genRand(1024)},
		&RawTest{"ID_RAW_2", genRand(1024 * 1024 * 30)},
	}
}

func genRand(size int) []byte {
	buf := make([]byte, size)
	rand.Read(buf)
	return buf
}
