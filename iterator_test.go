package gotinydb

import (
	"reflect"
	"testing"
)

func TestIterator(t *testing.T) {
	defer clean()
	err := open(t)
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
