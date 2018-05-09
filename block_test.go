package db

import (
	"os"
	"reflect"
	"testing"
)

func TestBlock(t *testing.T) {
	path := os.TempDir() + "/blockTest"
	defer os.RemoveAll(path)

	b := NewIndex(path)

	for _, val := range testList() {
		b.tree.Put(val[0], val[1])
	}

	if listLen := len(testList()); listLen != b.tree.Size() {
		t.Errorf("the tree has %d element(s) but the list is %d", b.tree.Size(), listLen)
		return
	}

	for _, val := range testList() {
		savedObj, found := b.tree.Get(val[0])
		if !found {
			t.Errorf("ID %q is not found", val[0])
			return
		}

		if !reflect.DeepEqual(val[1], savedObj) {
			t.Errorf("saved value is not equal: \n\t%v\n\t%v", val[1], savedObj)
			return
		}
	}

	saveErr := b.Save()
	if saveErr != nil {
		t.Errorf("save in %q err: %s", b.path, saveErr.Error())
		return
	}

	b.tree = nil
	b = NewIndex(path)

	loadErr := b.Load()
	if loadErr != nil {
		t.Errorf("loading tree: %s", loadErr.Error())
		return
	}

	for _, val := range testList() {
		savedID, found := b.tree.Get(val[0])
		if !found {
			t.Errorf("ID %q is not found", val[0])
			return
		}

		if !reflect.DeepEqual(val[1], savedID) {
			t.Errorf("saved value is not equal: \n\t%s\n\t%s", val[1], savedID)
			return
		}
	}
}

func testList() [][]string {
	return [][]string{
		[]string{"indexed field value 1", "id1"},
		[]string{"indexed field value 2", "id2"},
	}
}
