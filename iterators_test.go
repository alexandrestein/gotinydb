package gotinydb

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"
)

func TestCollectionIterator(t *testing.T) {
	defer clean()
	err := openT(t)
	if err != nil {
		return
	}

	iter := testCol.GetIterator()
	n := 0
	for ; iter.Valid(); iter.Next() {
		n++
	}
	iter.Close()

	if n != 2 {
		t.Errorf("regular iterator returned %d document but expect %d", n, 2)
	}

	iter = testCol.GetRevertedIterator()
	n = 0
	for ; iter.Valid(); iter.Next() {
		n++
	}
	iter.Close()

	if n != 2 {
		t.Errorf("reverted iterator returned %d document but expect %d", n, 2)
	}

	iter = testCol.GetIterator()
	n = 0
	last := new(testUserStruct)
	for iter.Seek(cloneTestUserID); iter.Valid(); iter.Next() {
		iter.GetValue(last)
		n++
	}
	if n != 1 {
		t.Errorf("regular iterator after seek returned %d document but expect %d", n, 1)
	}
	if !reflect.DeepEqual(last, cloneTestUser) {
		t.Errorf("regular iterator after seek returned %v document but expect %v", last, cloneTestUser)
	}
	iter.Close()
}

func TestFileIterator(t *testing.T) {
	defer clean()
	err := openT(t)
	if err != nil {
		return
	}

	for i := 0; i < 5; i++ {
		randBuff := make([]byte, 20*1000*1000)
		rand.Read(randBuff)
		buff := bytes.NewBuffer(randBuff)
		_, err := testDB.GetFileStore().PutFile(fmt.Sprint(i), fmt.Sprint(i), buff)
		if err != nil {
			t.Fatalf("can't write the file %d: %s", i, err.Error())
		}
	}

	iter := testDB.GetFileStore().GetFileIterator()
	defer iter.Close()

	expectedOrder := []string{"0", "2", "3", "1", "4"}
	n := 0
	for ; iter.Valid(); iter.Next() {
		meta := iter.GetMeta()
		if expectedOrder[n] != meta.ID {
			t.Errorf("the expected ID is %q but got %q", expectedOrder[n], meta.ID)
		}
		if expectedOrder[n] != meta.Name {
			t.Errorf("the expected name is %q but got %q", expectedOrder[n], meta.Name)
		}
		n++
	}

	if n != 5 {
		t.Errorf("this test must loop %d times but it looks like it did only %d", 5, n)
	}

	iter.Seek("3")
	n = 2
	for ; iter.Valid(); iter.Next() {
		meta := iter.GetMeta()
		if expectedOrder[n] != meta.ID {
			t.Errorf("the expected ID is %q but got %q", expectedOrder[n], meta.ID)
		}
		if expectedOrder[n] != meta.Name {
			t.Errorf("the expected name is %q but got %q", expectedOrder[n], meta.Name)
		}
		n++
	}

	if n != 5 {
		t.Errorf("this test must loop %d times but it looks like it did only %d", 5, n)
	}
}
