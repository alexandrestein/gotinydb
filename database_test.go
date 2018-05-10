package db

import (
	"bytes"
	"os"
	"reflect"
	"testing"
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
