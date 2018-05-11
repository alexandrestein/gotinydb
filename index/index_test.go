package index

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

var (
	path = os.TempDir() + "/dbTest"
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
		index.Put(val[0], val[1].(string))
	}

	if listLen := len(list); listLen != index.getTree().Size() {
		t.Errorf("the tree has %d element(s) but the list is %d", index.getTree().Size(), listLen)
		return
	}

	saveErr := index.Save()
	if saveErr != nil {
		t.Errorf("save in %q err: %s", index.getPath(), saveErr.Error())
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

func TestNeighboursWithString(t *testing.T) {
	i := NewStringIndex(path)
	list := testStringList()
	for _, val := range list {
		i.Put(val[0], val[1].(string))
	}

	testNeighbours(t, i, "indexed field value m", 5, 11, true)
	testNeighbours(t, i, "indexed field value a", 5, 6, true)
	testNeighbours(t, i, "indexed field value", 5, 5, false)
	testNeighbours(t, i, "indexed field value z", 5, 6, true)
	testNeighbours(t, i, "indexed field value mm", 1, 2, false)

	// This is not testable because the value needs to be founded or not at the
	// end to get really precise wanted neighbours.
	// testNeighbours(t, i, "indexed field value za", 1, 2, false)
}

func testNeighbours(t *testing.T, i Index, key interface{}, nbToTry, nbToGet int, needToFound bool) {
	keys, values, found := i.GetNeighbours(key, nbToTry, nbToTry)
	if found != needToFound {
		if needToFound {
			t.Errorf("The key %v is not found", key)
		} else {
			t.Errorf("The key %v is found", key)
		}
		return
	}

	if len(keys) != nbToGet {
		fmt.Println(values)
		t.Errorf("The key count is not good, expecting %d and had %d", nbToGet, len(keys))
		return
	}
	if len(values) != nbToGet {
		t.Errorf("The value count is not good, expecting %d and had %d", nbToGet, len(values))
		return
	}
}

func testStringList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{"indexed field value a", "id0"},
		[]interface{}{"indexed field value b", "id1"},
		[]interface{}{"indexed field value c", "id2"},
		[]interface{}{"indexed field value d", "id3"},
		[]interface{}{"indexed field value e", "id4"},
		[]interface{}{"indexed field value f", "id5"},
		[]interface{}{"indexed field value g", "id6"},
		[]interface{}{"indexed field value h", "id7"},
		[]interface{}{"indexed field value i", "id8"},
		[]interface{}{"indexed field value j", "id9"},
		[]interface{}{"indexed field value k", "id10"},
		[]interface{}{"indexed field value l", "id11"},
		[]interface{}{"indexed field value m", "id12"},
		[]interface{}{"indexed field value n", "id13"},
		[]interface{}{"indexed field value o", "id14"},
		[]interface{}{"indexed field value p", "id15"},
		[]interface{}{"indexed field value q", "id16"},
		[]interface{}{"indexed field value r", "id17"},
		[]interface{}{"indexed field value s", "id18"},
		[]interface{}{"indexed field value t", "id19"},
		[]interface{}{"indexed field value u", "id20"},
		[]interface{}{"indexed field value v", "id21"},
		[]interface{}{"indexed field value w", "id22"},
		[]interface{}{"indexed field value x", "id23"},
		[]interface{}{"indexed field value y", "id24"},
		[]interface{}{"indexed field value z", "id25"},
	}
}

func testIntList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{1, "id1"},
		[]interface{}{2, "id2"},
	}
}
