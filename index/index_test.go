package index

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"gitea.interlab-net.com/alexandre/db/query"
	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
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

	// Try to add the value twice this should do nothing
	lenBeforeDuplicateInsert := index.getTree().Size()
	index.Put(list[0][0], list[0][1].(string))
	if lenBeforeDuplicateInsert != index.getTree().Size() {
		t.Errorf("insertion of same id at the same place should be baned")
		return
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
	defer os.RemoveAll(internalTesting.Path)
	list := getGoodList(index)

	loadErr := index.Load()
	if loadErr != nil {
		t.Errorf("loading tree: %s", loadErr.Error())
		return
	}

	for _, val := range list {
		savedIDs, found := index.Get(val[0])
		if !found {
			t.Errorf("ID %q is not found", val[0])
			return
		}

		if !reflect.DeepEqual(val[1], savedIDs[0]) {
			t.Errorf("saved value is not equal: \n\t%s\n\t%s", val[1], savedIDs)
			return
		}
	}
}

func TestStringIndex(t *testing.T) {
	i := NewStringIndex(internalTesting.Path, []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewStringIndex(internalTesting.Path, []string{})
	testLoadIndex(t, i)
}

func TestIntIndex(t *testing.T) {
	i := NewIntIndex(internalTesting.Path, []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewIntIndex(internalTesting.Path, []string{})
	testLoadIndex(t, i)
}

func TestNeighboursWithString(t *testing.T) {
	i := NewStringIndex(internalTesting.Path, []string{})
	i.getTree().Clear()
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
		t.Errorf("The key count is not good, expecting %d and had %d", nbToGet, len(keys))
		return
	}
	if len(values) != nbToGet {
		t.Errorf("The value count is not good, expecting %d and had %d", nbToGet, len(values))
		return
	}
}

func TestRemoveIdFromAll(t *testing.T) {
	i := NewStringIndex(internalTesting.Path, []string{})
	i.getTree().Clear()
	list := testStringList()

	list = append(list, testSameValueStringList()...)

	for _, val := range list {
		i.Put(val[0], val[1].(string))
	}

	for _, val := range list {
		rmErr := i.RemoveIDFromAll(val[1].(string))
		if rmErr != nil {
			t.Errorf("removing id %s: %s", val[1], rmErr.Error())
			return
		}
	}

	if size := i.getTree().Size(); size != 0 {
		t.Errorf("size must be 0 and has %d", size)
	}
}

func TestUpdate(t *testing.T) {
	i := NewStringIndex(internalTesting.Path, []string{})
	i.getTree().Clear()

	// Insert for the first time
	for _, val := range testStringList() {
		i.Put(val[0], val[1].(string))
	}
	for _, val := range testSameValueStringList() {
		i.Put(val[0], val[1].(string))
	}

	// Update with the oposite values
	for y, val := range testStringList() {
		i.Put(testStringList()[len(testStringList())-1-y][0], val[1].(string))
		i.RemoveID(val[0], val[1].(string))
	}

	// Do the checks
	for y, val := range testStringList() {
		// Get the ids back
		ids, found := i.Get(val[0])
		if !found {
			t.Errorf("not found")
			return
		}

		// Check that the first indexed value has the last id position
		if !reflect.DeepEqual(ids, []string{testStringList()[len(testStringList())-1-y][1].(string)}) {
			t.Errorf("update not done. Expecting %v and had: %v", testStringList()[len(testStringList())-1-y][1], ids)
			return
		}
	}
}

func TestDuplicatedStringValue(t *testing.T) {
	i := NewStringIndex(internalTesting.Path, []string{})
	i.getTree().Clear()

	// Add the regular ones
	for _, val := range testStringList() {
		i.Put(val[0], val[1].(string))
	}

	list := testSameValueStringList()
	for _, val := range list {
		i.Put(val[0], val[1].(string))
	}

	if i.getTree().Size() != 27 {
		t.Errorf("the size must be 27 and is %d", i.getTree().Size())
		return
	}

	ids, found := i.Get("multiple IDs indexed")
	if !found {
		t.Errorf("the ids are not found by the indexed value")
		return
	}

	if !reflect.DeepEqual(ids, []string{"id100", "id110", "id120", "id130", "id140", "id150", "id160", "id170", "id180", "id190"}) {
		t.Errorf("the ids are not correct: %v", ids)
		return
	}
}

func TestApply(t *testing.T) {
	i := NewStringIndex(internalTesting.Path, []string{"Add", "Street", "Name"})

	objs := internalTesting.GetCompleteUsersExampleOneAndTow()

	for j, obj := range objs {
		if j == 0 {
			if _, apply := i.Apply(obj); !apply {
				buf, _ := json.Marshal(obj)
				t.Errorf("the index %v must apply to %v but not", i.GetSelector(), string(buf))
				return
			}
		} else {
			if _, apply := i.Apply(obj); apply {
				buf, _ := json.Marshal(obj)
				t.Errorf("the index %v must not apply to %v but not", i.GetSelector(), string(buf))
				return
			}
		}
	}
}

func TestStringQuery(t *testing.T) {
	selector := []string{"Add", "Street", "Name"}
	i := NewStringIndex(internalTesting.Path, selector)
	user0 := &internalTesting.CompleteUser{}
	for n, val := range internalTesting.GetCompleteUsersExampleStreetNamesOnly() {
		user := val.(*internalTesting.CompleteUser)
		if n == 0 {
			user0 = user
		}
		i.Put(user.Add.Street.Name, val.GetID())
	}

	q := query.NewQuery(selector)
	a1 := query.NewAction().Do(query.Equal).CompareTo(user0.Add.Street.Name)
	q.AddAction(a1)
	ids := i.RunQuery(q)
	if len(ids) != 1 || ids[0] != user0.GetID() {
		t.Errorf("the returned id %q is not good.", ids)
	}

	q = query.NewQuery(selector).SetLimit(10)
	q.AddAction(query.NewAction().Do(query.Greater).CompareTo("East street"))
	ids = i.RunQuery(q)
	fmt.Println("1", ids)

	q = query.NewQuery(selector).SetLimit(3)
	q.AddAction(query.NewAction().Do(query.Greater).CompareTo("East street"))
	q.AddAction(query.NewAction().Do(query.NotEqual))
	ids = i.RunQuery(q)
	fmt.Println("2", ids)

	q = query.NewQuery(selector).SetLimit(10)
	q.AddAction(query.NewAction().Do(query.Less).CompareTo("West street"))
	ids = i.RunQuery(q)
	fmt.Println("3", ids)

	q = query.NewQuery(selector).SetLimit(3)
	q.AddAction(query.NewAction().Do(query.Less).CompareTo("West street"))
	q.AddAction(query.NewAction().Do(query.NotEqual))
	ids = i.RunQuery(q)
	fmt.Println("4", ids)

	q = query.NewQuery(selector).SetLimit(3)
	q.AddAction(query.NewAction())
	ids = i.RunQuery(q)
	fmt.Println("5", ids)

	i.getTree().Clear()
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

func testSameValueStringList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{"multiple IDs indexed", "id100"},
		[]interface{}{"multiple IDs indexed", "id110"},
		[]interface{}{"multiple IDs indexed", "id120"},
		[]interface{}{"multiple IDs indexed", "id130"},
		[]interface{}{"multiple IDs indexed", "id140"},
		[]interface{}{"multiple IDs indexed", "id150"},
		[]interface{}{"multiple IDs indexed", "id160"},
		[]interface{}{"multiple IDs indexed", "id170"},
		[]interface{}{"multiple IDs indexed", "id180"},
		[]interface{}{"multiple IDs indexed", "id190"},
	}
}

func testIntList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{1, "id1"},
		[]interface{}{2, "id2"},
	}
}
