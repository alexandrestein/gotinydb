package GoTinyDB

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/alexandreStein/gods/utils"
)

type (
	TestStruct struct {
		IndexedValue interface{}
		ObjectID     string
	}
)

func getGoodList(i Index) []*TestStruct {
	switch i.Type() {
	case utils.StringComparatorType:
		return testStringList()
	case utils.IntComparatorType:
		return testIntList()
	case utils.Int8ComparatorType:
		return testInt8List()
	case utils.Int16ComparatorType:
		return testInt16List()
	case utils.Int32ComparatorType:
		return testInt32List()
	case utils.Int64ComparatorType:
		return testInt64List()
	case utils.UIntComparatorType:
		return testUIntList()
	case utils.UInt8ComparatorType:
		return testUInt8List()
	case utils.UInt16ComparatorType:
		return testUInt16List()
	case utils.UInt32ComparatorType:
		return testUInt32List()
	case utils.UInt64ComparatorType:
		return testUInt64List()
	case utils.Float32ComparatorType:
		return testFloat32List()
	case utils.Float64ComparatorType:
		return testFloat64List()
	case utils.TimeComparatorType:
		return testTimeList()
	default:
		return nil
	}
}

func testApply(t *testing.T, i Index) {
	list := getGoodList(i)

	for _, obj := range list {
		if _, apply := i.Apply(obj); !apply {
			buf, _ := json.Marshal(obj)
			t.Errorf("the index %q must apply to %v but not", i.GetSelector(), string(buf))
		}
	}
}

func testSaveIndex(t *testing.T, index Index) []byte {
	list := getGoodList(index)
	for _, val := range list {
		index.Put(val.IndexedValue, val.ObjectID)
	}

	testApply(t, index)

	// Try to add the value twice this should do nothing
	lenBeforeDuplicateInsert := index.getTree().Size()
	index.Put(list[0].IndexedValue, list[0].ObjectID)
	if lenBeforeDuplicateInsert != index.getTree().Size() {
		t.Errorf("insertion of same id at the same place should be baned")
		return nil
	}

	if listLen := len(list); listLen != index.getTree().Size() {
		t.Errorf("the tree has %d element(s) but the list is %d", index.getTree().Size(), listLen)
		return nil
	}

	indexContentToSave, saveErr := index.Save()
	if saveErr != nil {
		t.Errorf("save in %q err: %s", index.getName(), saveErr.Error())
		return nil
	}

	return indexContentToSave
}

func testLoadIndex(t *testing.T, index Index, indexSavedContent []byte) {
	list := getGoodList(index)

	loadErr := index.Load(indexSavedContent)
	if loadErr != nil {
		t.Errorf("loading tree: %s", loadErr.Error())
		return
	}

	for _, val := range list {
		savedIDs, found := index.Get(val.IndexedValue)
		if !found {
			t.Errorf("ID %q is not found", val.IndexedValue)
			return
		}

		if !reflect.DeepEqual(val.ObjectID, savedIDs[0]) {
			t.Errorf("saved value is not equal: \n\t%s\n\t%s", val.ObjectID, savedIDs)
			return
		}
	}
}

func TestStringIndex(t *testing.T) {
	i := NewStringIndex(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewStringIndex(Path, []string{})
	testLoadIndex(t, i, indexContent)
}

func TestIntIndex(t *testing.T) {
	i := NewIntIndex(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewIntIndex(Path, []string{})
	testLoadIndex(t, i, indexContent)
}

func TestInt8Index(t *testing.T) {
	i := NewInt8Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt8Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestInt16Index(t *testing.T) {
	i := NewInt16Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt16Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestInt32Index(t *testing.T) {
	i := NewInt32Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt32Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestInt64Index(t *testing.T) {
	i := NewInt64Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt64Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestUIntIndex(t *testing.T) {
	i := NewUintIndex(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUintIndex(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestUInt8Index(t *testing.T) {
	i := NewUint8Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint8Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestUInt16Index(t *testing.T) {
	i := NewUint16Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint16Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestUInt32Index(t *testing.T) {
	i := NewUint32Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint32Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestUInt64Index(t *testing.T) {
	i := NewUint64Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint64Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestFloat32Index(t *testing.T) {
	i := NewFloat32Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewFloat32Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestFloat64Index(t *testing.T) {
	i := NewFloat64Index(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewFloat64Index(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestTimeIndex(t *testing.T) {
	i := NewTimeIndex(Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	indexContent := testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewTimeIndex(Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i, indexContent)
}

func TestUpdate(t *testing.T) {
	i := NewStringIndex(Path+"/indexTest", []string{})
	i.getTree().Clear()

	// Insert for the first time
	for _, val := range testStringList() {
		i.Put(val.IndexedValue, val.ObjectID)
	}
	for _, val := range testSameValueStringList() {
		i.Put(val.IndexedValue, val.ObjectID)
	}

	// Update with the oposite values
	for y, val := range testStringList() {
		i.Put(testStringList()[len(testStringList())-1-y].IndexedValue, val.ObjectID)
		i.RemoveID(val.IndexedValue, val.ObjectID)
	}

	// Do the checks
	for y, val := range testStringList() {
		// Get the ids back
		ids, found := i.Get(val.IndexedValue)
		if !found {
			t.Errorf("not found")
			return
		}

		// Check that the first indexed value has the last id position
		if !reflect.DeepEqual(ids, []string{testStringList()[len(testStringList())-1-y].ObjectID}) {
			t.Errorf("update not done. Expecting %v and had: %v", testStringList()[len(testStringList())-1-y].ObjectID, ids)
			return
		}
	}
}

func TestDuplicatedStringValue(t *testing.T) {
	i := NewStringIndex(Path+"/indexTest", []string{})
	i.getTree().Clear()

	// Add the regular ones
	for _, val := range testStringList() {
		i.Put(val.IndexedValue, val.ObjectID)
	}

	list := testSameValueStringList()
	for _, val := range list {
		i.Put(val.IndexedValue, val.ObjectID)
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

func testGetAllLists() (ret []*TestStruct) {
	ret = append(ret, testStringList()...)
	ret = append(ret, testSameValueStringList()...)
	ret = append(ret, testIntList()...)
	ret = append(ret, testTimeList()...)
	ret = append(ret, testInt8List()...)
	ret = append(ret, testInt16List()...)
	ret = append(ret, testInt32List()...)
	ret = append(ret, testInt64List()...)
	ret = append(ret, testUIntList()...)
	ret = append(ret, testUInt8List()...)
	ret = append(ret, testUInt16List()...)
	ret = append(ret, testUInt32List()...)
	ret = append(ret, testUInt64List()...)
	ret = append(ret, testFloat32List()...)
	ret = append(ret, testFloat64List()...)
	return
}

func testStringList() []*TestStruct {
	return []*TestStruct{
		{"indexed field value a", "id0"},
		{"indexed field value b", "id1"},
		{"indexed field value c", "id2"},
		{"indexed field value d", "id3"},
		{"indexed field value e", "id4"},
		{"indexed field value f", "id5"},
		{"indexed field value g", "id6"},
		{"indexed field value h", "id7"},
		{"indexed field value i", "id8"},
		{"indexed field value j", "id9"},
		{"indexed field value k", "id10"},
		{"indexed field value l", "id11"},
		{"indexed field value m", "id12"},
		{"indexed field value n", "id13"},
		{"indexed field value o", "id14"},
		{"indexed field value p", "id15"},
		{"indexed field value q", "id16"},
		{"indexed field value r", "id17"},
		{"indexed field value s", "id18"},
		{"indexed field value t", "id19"},
		{"indexed field value u", "id20"},
		{"indexed field value v", "id21"},
		{"indexed field value w", "id22"},
		{"indexed field value x", "id23"},
		{"indexed field value y", "id24"},
		{"indexed field value z", "id25"},
	}
}

func testSameValueStringList() []*TestStruct {
	return []*TestStruct{
		{"multiple IDs indexed", "id100"},
		{"multiple IDs indexed", "id110"},
		{"multiple IDs indexed", "id120"},
		{"multiple IDs indexed", "id130"},
		{"multiple IDs indexed", "id140"},
		{"multiple IDs indexed", "id150"},
		{"multiple IDs indexed", "id160"},
		{"multiple IDs indexed", "id170"},
		{"multiple IDs indexed", "id180"},
		{"multiple IDs indexed", "id190"},
	}
}

// func TestStringQuery(t *testing.T) {
// 	selector := []string{"Add", "Street", "Name"}
// 	i := NewStringIndex(Path, selector)
// 	for _, val := range internalTesting.GetCompleteUsersExampleStreetNamesOnly() {
// 		user := val.(*internalTesting.CompleteUser)
// 		i.Put(user.Add.Street.Name, val.GetID())
// 	}
//
// 	buildTestQuery := func(limit int, reverted bool, getAction, keepAction *query.Action) *query.Query {
// 		if limit == 0 {
// 			limit = 1
// 		}
// 		q := query.NewQuery().SetLimit(limit)
// 		if reverted {
// 			q.InvertOrder()
// 		}
// 		if getAction != nil {
// 			getAction.Selector = selector
// 		}
// 		q.GetActions = []*query.Action{getAction}
// 		if keepAction != nil {
// 			keepAction.Selector = selector
// 		}
// 		q.KeepActions = []*query.Action{keepAction}
// 		return q
// 	}
//
// 	listOfTests := []struct {
// 		name           string
// 		query          *query.Query
// 		expectedResult []string
// 	}{
// 		{
// 			name:           "Get Equal with limit 1",
// 			query:          buildTestQuery(1, false, query.NewAction(query.Equal).CompareTo("North street"), nil),
// 			expectedResult: []string{"S_North_1"},
// 		}, {
// 			name:           "Get Equal with limit 5",
// 			query:          buildTestQuery(5, false, query.NewAction(query.Equal).CompareTo("North street"), nil),
// 			expectedResult: []string{"S_North_1", "S_North_6", "S_North_11", "S_North_16", "S_North_21"},
// 		}, {
// 			name:           "Get Equal with limit 5 and reverted",
// 			query:          buildTestQuery(5, true, query.NewAction(query.Equal).CompareTo("North street"), nil),
// 			expectedResult: []string{"S_North_21", "S_North_16", "S_North_11", "S_North_6", "S_North_1"},
// 		}, {
// 			name:           "Get Greater and Equal - limit 15",
// 			query:          buildTestQuery(15, false, query.NewAction(query.Greater).CompareTo("East street"), nil),
// 			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49", "S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25"},
// 		}, {
// 			name:           "Get Greater - limit 10",
// 			query:          buildTestQuery(10, false, query.NewAction(query.Greater).CompareTo("East street").EqualWanted(), nil),
// 			expectedResult: []string{"S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25", "S_George_30", "S_George_35", "S_George_40", "S_George_45", "S_North_21"},
// 		}, {
// 			name:           "Get Less and Equal - limit 15",
// 			query:          buildTestQuery(15, false, query.NewAction(query.Less).CompareTo("West street"), nil),
// 			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23", "S_West_28", "S_West_33", "S_West_38", "S_West_43", "S_West_48", "S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22"},
// 		}, {
// 			name:           "Get Less - limit 10",
// 			query:          buildTestQuery(10, false, query.NewAction(query.Less).CompareTo("West street").EqualWanted(), nil),
// 			expectedResult: []string{"S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22", "S_South_27", "S_South_32", "S_South_37", "S_South_42", "S_South_47"},
// 		}, {
// 			name:           "Empty Action",
// 			query:          buildTestQuery(10, false, nil, nil),
// 			expectedResult: []string{},
// 		}, {
// 			name:           "Greater than last",
// 			query:          buildTestQuery(5, false, query.NewAction(query.Greater).CompareTo("Z"), nil),
// 			expectedResult: []string{},
// 		}, {
// 			name:           "Less than first",
// 			query:          buildTestQuery(5, false, query.NewAction(query.Less).CompareTo("A"), nil),
// 			expectedResult: []string{},
// 		}, {
// 			name:           "Greater from start",
// 			query:          buildTestQuery(5, false, query.NewAction(query.Greater).CompareTo("A"), nil),
// 			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24"},
// 		}, {
// 			name:           "Less from end",
// 			query:          buildTestQuery(5, false, query.NewAction(query.Less).CompareTo("Z"), nil),
// 			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23"},
// 		}, {
// 			name:           "Greater from start and keep after E - limit 20",
// 			query:          buildTestQuery(100, false, query.NewAction(query.Greater).CompareTo("A"), query.NewAction(query.Greater).CompareTo("F")),
// 			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49"},
// 		}, {
// 			name:           "Duplicated",
// 			query:          buildTestQuery(10, false, query.NewAction(query.Equal).CompareTo("North street Dup"), query.NewAction(query.Greater).CompareTo("North street Dup4")).DistinctWanted(),
// 			expectedResult: []string{"DUP_1"},
// 		},
// 	}
//
// 	for _, test := range listOfTests {
// 		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
// 		defer cancel()
// 		idsChan := i.RunQuery(ctx, test.query.GetActions)
// 		var ids []string
// 		select {
// 		case ids = <-idsChan:
// 		case <-ctx.Done():
// 			break
// 		}
// 		if !reflect.DeepEqual(test.expectedResult, ids) {
// 			if len(test.expectedResult) == 0 && len(ids) == 0 {
// 				continue
// 			}
// 			t.Errorf("%q the expected result is %v but had %v", test.name, test.expectedResult, ids)
// 			break
// 		}
// 	}
// 	i.getTree().Clear()
// }

// func testStringList() []*TestStruct {
// 	return []*TestStruct{
// 		{"1", "id1"},
// 		{"2", "id2"},
// 	}
// }

func testIntList() []*TestStruct {
	return []*TestStruct{
		{1, "id1"},
		{2, "id2"},
	}
}

func testInt8List() []*TestStruct {
	return []*TestStruct{
		{int8(1), "id1"},
		{int8(2), "id2"},
	}
}

func testInt16List() []*TestStruct {
	return []*TestStruct{
		{int16(1), "id1"},
		{int16(2), "id2"},
	}
}
func testInt32List() []*TestStruct {
	return []*TestStruct{
		{int32(1), "id1"},
		{int32(2), "id2"},
	}
}
func testInt64List() []*TestStruct {
	return []*TestStruct{
		{int64(1), "id1"},
		{int64(2), "id2"},
	}
}
func testUIntList() []*TestStruct {
	return []*TestStruct{
		{uint(1), "id1"},
		{uint(2), "id2"},
	}
}
func testUInt8List() []*TestStruct {
	return []*TestStruct{
		{uint8(1), "id1"},
		{uint8(2), "id2"},
	}
}
func testUInt16List() []*TestStruct {
	return []*TestStruct{
		{uint16(1), "id1"},
		{uint16(2), "id2"},
	}
}
func testUInt32List() []*TestStruct {
	return []*TestStruct{
		{uint32(1), "id1"},
		{uint32(2), "id2"},
	}
}
func testUInt64List() []*TestStruct {
	return []*TestStruct{
		{uint64(1), "id1"},
		{uint64(2), "id2"},
	}
}
func testFloat32List() []*TestStruct {
	return []*TestStruct{
		{float32(0.1), "id1"},
		{float32(0.2), "id2"},
	}
}
func testFloat64List() []*TestStruct {
	return []*TestStruct{
		{float64(0.1), "id1"},
		{float64(0.2), "id2"},
	}
}
func testTimeList() []*TestStruct {
	return []*TestStruct{
		{time.Now().Add(-time.Hour).Truncate(time.Second), "id1"},
		{time.Now().Add(time.Hour).Truncate(time.Second), "id2"},
	}
}
