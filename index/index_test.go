package index

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
	"github.com/emirpasic/gods/utils"
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
	case utils.TimeComparatorType:
		return testTimeList()
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
	}
	return nil
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

func testSaveIndex(t *testing.T, index Index) {
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
	list := getGoodList(index)

	loadErr := index.Load()
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
	i := NewString(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewString(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestIntIndex(t *testing.T) {
	i := NewInt(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestInt8Index(t *testing.T) {
	i := NewInt8(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt8(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestInt16Index(t *testing.T) {
	i := NewInt16(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt16(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestInt32Index(t *testing.T) {
	i := NewInt32(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt32(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestInt64Index(t *testing.T) {
	i := NewInt64(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt64(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestUIntIndex(t *testing.T) {
	i := NewUint(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestUInt8Index(t *testing.T) {
	i := NewUint8(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint8(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestUInt16Index(t *testing.T) {
	i := NewUint16(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint16(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestUInt32Index(t *testing.T) {
	i := NewUint32(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint32(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestUInt64Index(t *testing.T) {
	i := NewUint64(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint64(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestFloat32Index(t *testing.T) {
	i := NewFloat32(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewFloat32(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestFloat64Index(t *testing.T) {
	i := NewFloat64(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewFloat64(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestTimeIndex(t *testing.T) {
	i := NewTime(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewTime(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	testLoadIndex(t, i)
}

func TestRemoveIdFromAll(t *testing.T) {
	i := NewString(internalTesting.Path+"/indexTest", []string{"IndexedValue"})
	i.getTree().Clear()
	list := testStringList()

	list = append(list, testSameValueStringList()...)

	for _, val := range list {
		i.Put(val.IndexedValue, val.ObjectID)
	}

	for _, val := range list {
		rmErr := i.RemoveIDFromAll(val.ObjectID)
		if rmErr != nil {
			t.Errorf("removing id %s: %s", val.ObjectID, rmErr.Error())
			return
		}
	}

	if size := i.getTree().Size(); size != 0 {
		t.Errorf("size must be 0 and has %d", size)
	}
}

func TestUpdate(t *testing.T) {
	i := NewString(internalTesting.Path+"/indexTest", []string{})
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
	i := NewString(internalTesting.Path+"/indexTest", []string{})
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
		&TestStruct{"indexed field value a", "id0"},
		&TestStruct{"indexed field value b", "id1"},
		&TestStruct{"indexed field value c", "id2"},
		&TestStruct{"indexed field value d", "id3"},
		&TestStruct{"indexed field value e", "id4"},
		&TestStruct{"indexed field value f", "id5"},
		&TestStruct{"indexed field value g", "id6"},
		&TestStruct{"indexed field value h", "id7"},
		&TestStruct{"indexed field value i", "id8"},
		&TestStruct{"indexed field value j", "id9"},
		&TestStruct{"indexed field value k", "id10"},
		&TestStruct{"indexed field value l", "id11"},
		&TestStruct{"indexed field value m", "id12"},
		&TestStruct{"indexed field value n", "id13"},
		&TestStruct{"indexed field value o", "id14"},
		&TestStruct{"indexed field value p", "id15"},
		&TestStruct{"indexed field value q", "id16"},
		&TestStruct{"indexed field value r", "id17"},
		&TestStruct{"indexed field value s", "id18"},
		&TestStruct{"indexed field value t", "id19"},
		&TestStruct{"indexed field value u", "id20"},
		&TestStruct{"indexed field value v", "id21"},
		&TestStruct{"indexed field value w", "id22"},
		&TestStruct{"indexed field value x", "id23"},
		&TestStruct{"indexed field value y", "id24"},
		&TestStruct{"indexed field value z", "id25"},
	}
}

func testSameValueStringList() []*TestStruct {
	return []*TestStruct{
		&TestStruct{"multiple IDs indexed", "id100"},
		&TestStruct{"multiple IDs indexed", "id110"},
		&TestStruct{"multiple IDs indexed", "id120"},
		&TestStruct{"multiple IDs indexed", "id130"},
		&TestStruct{"multiple IDs indexed", "id140"},
		&TestStruct{"multiple IDs indexed", "id150"},
		&TestStruct{"multiple IDs indexed", "id160"},
		&TestStruct{"multiple IDs indexed", "id170"},
		&TestStruct{"multiple IDs indexed", "id180"},
		&TestStruct{"multiple IDs indexed", "id190"},
	}
}

func testIntList() []*TestStruct {
	return []*TestStruct{
		&TestStruct{1, "id1"},
		&TestStruct{2, "id2"},
	}
}

func testTimeList() []*TestStruct {
	return []*TestStruct{
		&TestStruct{time.Now().Add(time.Second * 10).Truncate(time.Second), "id1"},
		&TestStruct{time.Now().Add(time.Second * -10).Truncate(time.Second), "id2"},
	}
}

func testInt8List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{int8(1), "id1"},
		&TestStruct{int8(2), "id2"},
	}
}

func testInt16List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{int16(1), "id1"},
		&TestStruct{int16(2), "id2"},
	}
}
func testInt32List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{int32(1), "id1"},
		&TestStruct{int32(2), "id2"},
	}
}
func testInt64List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{int64(1), "id1"},
		&TestStruct{int64(2), "id2"},
	}
}
func testUIntList() []*TestStruct {
	return []*TestStruct{
		&TestStruct{uint(1), "id1"},
		&TestStruct{uint(2), "id2"},
	}
}
func testUInt8List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{uint8(1), "id1"},
		&TestStruct{uint8(2), "id2"},
	}
}
func testUInt16List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{uint16(1), "id1"},
		&TestStruct{uint16(2), "id2"},
	}
}
func testUInt32List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{uint32(1), "id1"},
		&TestStruct{uint32(2), "id2"},
	}
}
func testUInt64List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{uint64(1), "id1"},
		&TestStruct{uint64(2), "id2"},
	}
}
func testFloat32List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{float32(0.1), "id1"},
		&TestStruct{float32(0.2), "id2"},
	}
}
func testFloat64List() []*TestStruct {
	return []*TestStruct{
		&TestStruct{float64(0.1), "id1"},
		&TestStruct{float64(0.2), "id2"},
	}
}
