package db

import (
	"bytes"
	"crypto/rand"
	"os"
	"reflect"
	"testing"
	"time"
)

var (
	path = os.TempDir() + "/dbTest"
)

func TestDB(t *testing.T) {
	db, initErr := New(path)
	if initErr != nil {
		t.Error(initErr.Error())
		return
	}
	defer db.Close()

	col1, col1Err := db.Use("col1")
	if col1Err != nil {
		t.Errorf("openning test collection: %s", col1Err.Error())
		return
	}

	for _, user := range getUsersExample() {
		col1.Put(user.ID, user)
		tmpUser := &UserTest{}
		getErr := col1.Get(user.ID, tmpUser)
		if getErr != nil {
			t.Errorf("getting the object: %s", getErr.Error())
		}

		if !reflect.DeepEqual(user, tmpUser) {
			t.Errorf("returned object is not equal: %v\n%v", user, tmpUser)
		}
	}
	for _, raw := range getRawExample() {
		col1.Put(raw.ID, raw.Content)
		buf := bytes.NewBuffer(nil)
		getErr := col1.Get(raw.ID, buf)
		if getErr != nil {
			t.Errorf("getting record: %s", getErr.Error())
			return
		}

		if buf.String() != string(raw.Content) {
			t.Errorf("returned raw value is not the same as the given one")
			return
		}
	}
}

func TestExistingDB(t *testing.T) {
	defer os.RemoveAll(path)

	db, initErr := New(path)
	if initErr != nil {
		t.Error(initErr.Error())
		return
	}
	defer db.Close()

	_, col1Err := db.Use("col1")
	if col1Err != nil {
		t.Errorf("openning test collection: %s", col1Err.Error())
		return
	}
}

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
	// Time is truncate because the JSON format do not support nanosecondes
	return []*UserTest{
		&UserTest{"ID_USER_1", "mister 1", "pass 1", time.Now().Truncate(time.Millisecond)},
		&UserTest{"ID_USER_2", "mister 2", "pass 2", time.Now().Add(time.Hour * 3600).Truncate(time.Millisecond)},
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
