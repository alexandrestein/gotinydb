package GoTinyDB

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	internalTesting "github.com/alexandreStein/GoTinyDB/testing"
<<<<<<< HEAD
	"github.com/alexandreStein/gods/utils"
=======
>>>>>>> indexes
)

var rawExamples = []internalTesting.TestValue{}

func TestIndex(t *testing.T) {
	defer os.RemoveAll(internalTesting.Path)
	db, _ := Open(internalTesting.Path)

	col, _ := db.Use("col1")
	setIndexErr := col.SetIndex("test index", utils.StringComparatorType, []string{"UserName"})
	if setIndexErr != nil {
		t.Error(setIndexErr)
		return
	}

	for _, user := range internalTesting.GetUsersExample() {
		putErr := col.Put(user.GetID(), user)
		if putErr != nil {
			t.Errorf("puting the object: %s", putErr.Error())
			return
		}
	}

	col.Query(q)
}

func TestDB(t *testing.T) {
	// defer os.RemoveAll(internalTesting.Path)
	db, initErr := Open(internalTesting.Path)
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

		if !user.IsEqual(tmpUser) {
			t.Errorf("returned objects are not equal: \n%v\n%v", user, tmpUser)
			return
		}
	}
	rawExamples = internalTesting.GetRawExample()
	for _, raw := range rawExamples {
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
	db, initErr := Open(internalTesting.Path)
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

	for _, user := range internalTesting.GetUsersExample() {
		tmpUser := &internalTesting.UserTest{}
		getErr := col1.Get(user.GetID(), tmpUser)
		if getErr != nil {
			t.Errorf("getting the object: %s", getErr.Error())
			return
		}

		if !user.IsEqual(tmpUser) {
			t.Errorf("returned objects are not equal: \n%v\n%v", user, tmpUser)
			return
		}
	}
	for _, raw := range rawExamples {
		buf := bytes.NewBuffer(nil)
		getErr := col1.Get(raw.GetID(), buf)
		if getErr != nil {
			t.Errorf("getting record: %s", getErr.Error())
			return
		}

		if buf.String() != string(raw.GetContent().([]byte)) {
			fmt.Printf("%x\n%x\n", buf.String(), string(raw.GetContent().([]byte)))
			t.Errorf("returned raw value is not the same as the given one")
			return
		}
	}
}
