package index

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
	"github.com/emirpasic/gods/utils"
)

func getGoodList(i Index) [][]interface{} {
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
	i := NewString(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewString(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestIntIndex(t *testing.T) {
	i := NewInt(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestTimeIndex(t *testing.T) {
	i := NewTime(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewTime(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestInt8Index(t *testing.T) {
	i := NewInt8(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt8(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestInt16Index(t *testing.T) {
	i := NewInt16(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt16(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestInt32Index(t *testing.T) {
	i := NewInt32(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt32(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestInt64Index(t *testing.T) {
	i := NewInt64(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewInt64(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestUIntIndex(t *testing.T) {
	i := NewUint(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestUInt8Index(t *testing.T) {
	i := NewUint8(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint8(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestUInt16Index(t *testing.T) {
	i := NewUint16(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint16(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestUInt32Index(t *testing.T) {
	i := NewUint32(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint32(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestUInt64Index(t *testing.T) {
	i := NewUint64(internalTesting.Path+"/indexTest", []string{})
	i.getTree().Clear()
	testSaveIndex(t, i)

	i.getTree().Clear()

	i = NewUint64(internalTesting.Path+"/indexTest", []string{})
	testLoadIndex(t, i)
}

func TestRemoveIdFromAll(t *testing.T) {
	i := NewString(internalTesting.Path+"/indexTest", []string{})
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
	i := NewString(internalTesting.Path+"/indexTest", []string{})
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
	i := NewString(internalTesting.Path+"/indexTest", []string{})
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
	i := NewString(internalTesting.Path+"/indexTest", []string{"Add", "Street", "Name"})

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

func testTimeList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{time.Now().Add(time.Second * 10).Truncate(time.Second), "id1"},
		[]interface{}{time.Now().Add(time.Second * -10).Truncate(time.Second), "id2"},
	}
}

func testInt8List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{int8(1), "id1"},
		[]interface{}{int8(2), "id2"},
	}
}

func testInt16List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{int16(1), "id1"},
		[]interface{}{int16(2), "id2"},
	}
}
func testInt32List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{int32(1), "id1"},
		[]interface{}{int32(2), "id2"},
	}
}
func testInt64List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{int64(1), "id1"},
		[]interface{}{int64(2), "id2"},
	}
}
func testUIntList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{uint(1), "id1"},
		[]interface{}{uint(2), "id2"},
	}
}
func testUInt8List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{uint8(1), "id1"},
		[]interface{}{uint8(2), "id2"},
	}
}
func testUInt16List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{uint16(1), "id1"},
		[]interface{}{uint16(2), "id2"},
	}
}
func testUInt32List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{uint32(1), "id1"},
		[]interface{}{uint32(2), "id2"},
	}
}
func testUInt64List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{uint64(1), "id1"},
		[]interface{}{uint64(2), "id2"},
	}
}
func testFloat32List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{float32(0.1), "id1"},
		[]interface{}{float32(0.2), "id2"},
	}
}
func testFloat64List() [][]interface{} {
	return [][]interface{}{
		[]interface{}{float64(0.1), "id1"},
		[]interface{}{float64(0.2), "id2"},
	}
}
