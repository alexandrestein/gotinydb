package db

import (
	"os"
	"reflect"
	"testing"
)

var (
	path = os.TempDir() + "/blockTest"
)

func getGoodList(i Index) [][]interface{} {
	switch i.Type() {
	case StringIndexType:
		return testStringList()
	case IntIndexType:
		return testIntList()
	}
	return nil
}

func testSaveIndex(t *testing.T, index Index) {
	list := getGoodList(index)
	for _, val := range list {
		index.Put(val[0], val[1])
	}

	if listLen := len(list); listLen != index.GetTree().Size() {
		t.Errorf("the tree has %d element(s) but the list is %d", index.GetTree().Size(), listLen)
		return
	}

	saveErr := index.Save()
	if saveErr != nil {
		t.Errorf("save in %q err: %s", index.GetPath(), saveErr.Error())
		return
	}
}

func testLoadIndex(t *testing.T, index Index) {
	defer os.RemoveAll(path)
	list := getGoodList(index)

	loadErr := index.Load()
	if loadErr != nil {
		t.Errorf("loading tree: %s", loadErr.Error())
		return
	}

	for _, val := range list {
		savedID, found := index.Get(val[0])
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

func TestStringIndex(t *testing.T) {
	i := NewStringIndex(path)
	testSaveIndex(t, i)

	i.tree = nil
	i = NewStringIndex(path)
	testLoadIndex(t, i)
}

func TestIntIndex(t *testing.T) {
	i := NewIntIndex(path)
	testSaveIndex(t, i)

	i.tree.Clear()

	i = NewIntIndex(path)
	testLoadIndex(t, i)
}

func testStringList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{"indexed field value 1", "id1"},
		[]interface{}{"indexed field value 2", "id2"},
	}
}

func testIntList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{1, "id1"},
		[]interface{}{2, "id2"},
	}
}
