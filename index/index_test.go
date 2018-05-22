package index

import (
	"encoding/json"
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
	for _, val := range internalTesting.GetCompleteUsersExampleStreetNamesOnly() {
		user := val.(*internalTesting.CompleteUser)
		i.Put(user.Add.Street.Name, val.GetID())
	}

	buildTestQuery := func(limit int, reverted bool, actions ...*query.Action) *query.Query {
		if limit == 0 {
			limit = 1
		}
		q := query.NewQuery(selector).SetLimit(limit)
		if reverted {
			q.InvertOrder()
		}
		for _, action := range actions {
			q.AddAction(action)
		}
		return q
	}

	listOfTests := []struct {
		name           string
		query          *query.Query
		expectedResult []string
	}{
		{
			name:           "Get Equal with limit 1",
			query:          buildTestQuery(1, false, query.NewAction().Get(query.Equal).CompareTo("North street")),
			expectedResult: []string{"S_North_1"},
		}, {
			name:           "Get Equal with limit 5",
			query:          buildTestQuery(5, false, query.NewAction().Get(query.Equal).CompareTo("North street")),
			expectedResult: []string{"S_North_1", "S_North_6", "S_North_11", "S_North_16", "S_North_21"},
		}, {
			name:           "Get Equal with limit 5 and reverted",
			query:          buildTestQuery(5, true, query.NewAction().Get(query.Equal).CompareTo("North street")),
			expectedResult: []string{"S_North_21", "S_North_16", "S_North_11", "S_North_6", "S_North_1"},
		}, {
			name:           "Get Greater and Equal - limit 15",
			query:          buildTestQuery(15, false, query.NewAction().Get(query.Greater).CompareTo("East street")),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49", "S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25"},
		}, {
			name:           "Get Greater - limit 10",
			query:          buildTestQuery(10, false, query.NewAction().Get(query.Greater).CompareTo("East street"), query.NewAction().Get(query.NotEqual)),
			expectedResult: []string{"S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25", "S_George_30", "S_George_35", "S_George_40", "S_George_45", "S_North_21"},
		}, {
			name:           "Get Less and Equal - limit 15",
			query:          buildTestQuery(15, false, query.NewAction().Get(query.Less).CompareTo("West street")),
			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23", "S_West_28", "S_West_33", "S_West_38", "S_West_43", "S_West_48", "S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22"},
		}, {
			name:           "Get Less - limit 10",
			query:          buildTestQuery(10, false, query.NewAction().Get(query.Less).CompareTo("West street"), query.NewAction().Get(query.NotEqual)),
			expectedResult: []string{"S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22", "S_South_27", "S_South_32", "S_South_37", "S_South_42", "S_South_47"},
		}, {
			name:           "Empty Action",
			query:          buildTestQuery(10, false, query.NewAction()),
			expectedResult: []string{},
		}, {
			name:           "Greater than last",
			query:          buildTestQuery(5, false, query.NewAction().Get(query.Greater).CompareTo("Z")),
			expectedResult: []string{},
		}, {
			name:           "Less than first",
			query:          buildTestQuery(5, false, query.NewAction().Get(query.Less).CompareTo("A")),
			expectedResult: []string{},
		}, {
			name:           "Greater from start",
			query:          buildTestQuery(5, false, query.NewAction().Get(query.Greater).CompareTo("A")),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24"},
		}, {
			name:           "Less from end",
			query:          buildTestQuery(5, false, query.NewAction().Get(query.Less).CompareTo("Z")),
			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23"},
		},
	}

	for _, test := range listOfTests {
		ids := i.RunQuery(test.query)
		if !reflect.DeepEqual(test.expectedResult, ids) {
			if len(test.expectedResult) == 0 && len(ids) == 0 {
				continue
			}
			t.Errorf("%q the expected result is %v but had %v", test.name, test.expectedResult, ids)
		}
	}
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
