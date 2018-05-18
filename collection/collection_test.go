package collection

import (
	"os"
	"reflect"
	"testing"

	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
)

func insertAndCheck(t *testing.T, col *Collection, values []internalTesting.TestValue) {
	insert(t, col, values)
	if t.Failed() {
		return
	}

	ids := []string{}
	for _, value := range values {
		ids = append(ids, value.GetID())
	}

	check(t, col, ids, values)
}

func updateAndCheck(t *testing.T, col *Collection, values []internalTesting.TestValue) {
	insert(t, col, values)
	if t.Failed() {
		return
	}

	ids := []string{}
	for i, value := range values {
		y := len(values) - 1 - i
		ids = append(ids, values[y].GetID())
		putErr := col.Put(value.GetID(), values[y].GetContent())
		if putErr != nil {
			t.Error(putErr)
			return
		}
	}

	check(t, col, ids, values)
}

func delete(t *testing.T, col *Collection, values []internalTesting.TestValue) {
	for _, value := range values {
		delErr := col.Delete(value.GetID())
		if delErr != nil {
			t.Error(delErr)
			return
		}
	}
}

func insert(t *testing.T, col *Collection, values []internalTesting.TestValue) {
	for _, value := range values {
		putErr := col.Put(value.GetID(), value.GetContent())
		if putErr != nil {
			t.Error(putErr)
			return
		}
	}
}

func check(t *testing.T, col *Collection, ids []string, values []internalTesting.TestValue) {
	for i, value := range values {
		gettedValue := value.New()
		getErr := col.Get(ids[i], gettedValue)
		if getErr != nil {
			t.Error(getErr)
			return
		}

		if !reflect.DeepEqual(value, gettedValue) {
			t.Errorf("%v and %v are not equal", value, gettedValue)
			return
		}
	}
}

func TestCollectionObject(t *testing.T) {
	defer os.RemoveAll(internalTesting.Path)
	col, newColErr := NewCollection(internalTesting.Path)
	if newColErr != nil {
		t.Error(newColErr)
		return
	}

	users := internalTesting.GetUsersExample()
	insertAndCheck(t, col, users)
	if t.Failed() {
		return
	}
	delete(t, col, users)
	if t.Failed() {
		return
	}

	updateAndCheck(t, col, users)
	if t.Failed() {
		return
	}
	delete(t, col, users)
	if t.Failed() {
		return
	}
}

func TestCollectionBin(t *testing.T) {
	defer os.RemoveAll(internalTesting.Path)
	col, newColErr := NewCollection(internalTesting.Path)
	if newColErr != nil {
		t.Error(newColErr)
		return
	}

	raw := internalTesting.GetRawExample()
	insertAndCheck(t, col, raw)
	if t.Failed() {
		return
	}
	delete(t, col, raw)
	if t.Failed() {
		return
	}

	updateAndCheck(t, col, raw)
	if t.Failed() {
		return
	}
	delete(t, col, raw)
	if t.Failed() {
		return
	}
}
