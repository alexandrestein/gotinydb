package db

import (
	"bytes"
	"os"
	"reflect"
	"testing"

	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
	"gitea.interlab-net.com/alexandre/db/testing/funcs"
)

func TestDB(t *testing.T) {
	defer os.RemoveAll(internalTesting.Path)
	db, initErr := New(internalTesting.Path)
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

	if err := funcs.SetIndexes(t, col1); err != nil {
		t.Errorf("can't set the index: %s", err.Error())
		return
	}

	for _, user := range internalTesting.GetUsersExample() {
		putErr := col1.Put(user.GetID(), user)
		if putErr != nil {
			t.Errorf("puting the object: %s", putErr.Error())
			return
		}
		tmpUser := &internalTesting.UserTest{}
		getErr := col1.Get(user.GetID(), tmpUser)
		if getErr != nil {
			t.Errorf("getting the object: %s", getErr.Error())
			return
		}

		if !reflect.DeepEqual(user, tmpUser) {
			t.Errorf("returned object is not equal: %v\n%v", user, tmpUser)
			return
		}
	}
	for _, raw := range internalTesting.GetRawExample() {
		col1.Put(raw.GetID(), raw.GetContent())
		buf := bytes.NewBuffer(nil)
		getErr := col1.Get(raw.GetID(), buf)
		if getErr != nil {
			t.Errorf("getting record: %s", getErr.Error())
			return
		}

		if buf.String() != string(raw.GetContent().([]byte)) {
			t.Errorf("returned raw value is not the same as the given one")
			return
		}
	}
}

func TestExistingDB(t *testing.T) {
	defer os.RemoveAll(internalTesting.Path)
	db, initErr := New(internalTesting.Path)
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
