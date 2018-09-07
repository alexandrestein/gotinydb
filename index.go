package gotinydb

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/fatih/structs"
)

// newIndex build a new Index pointer
func newIndex(name string, t IndexType, selector ...string) *indexType {
	ret := new(indexType)
	ret.Name = name
	ret.Selector = selector
	ret.SelectorHash = buildSelectorHash(selector)
	ret.Type = t

	return ret
}

// apply take the full object to add in the collection and check if is must be
// indexed or not. If the object needs to be indexed the value to index is returned as a byte slice.
func (i *indexType) apply(object interface{}) (contentToIndex [][]byte, ok bool) {
	if structs.IsStruct(object) {
		return i.applyToStruct(structs.New(object))
	}

	if mp, ok := object.(map[string]interface{}); ok {
		return i.applyToMap(mp)
	}

	return nil, false
}

func (i *indexType) applyToStruct(object *structs.Struct) (contentToIndex [][]byte, ok bool) {
	var field *structs.Field
	for j, fieldName := range i.Selector {
		if j == 0 {
			field, ok = object.FieldOk(fieldName)
			// Check the JSON tag
			if !ok {
				field, ok = i.testJSONTag(object.Fields(), fieldName)
			}
		} else {
			field, ok = field.FieldOk(fieldName)
			// Check the JSON tag
			if !ok {
				field, ok = i.testJSONTag(object.Fields(), fieldName)
			}
		}

		if !ok {
			// return i.testJSONTag(object)
			return nil, false
		}
	}
	return i.testType(field.Value())
}

func (i *indexType) testJSONTag(fields []*structs.Field, fieldName string) (field *structs.Field, ok bool) {
	for _, tagField := range fields {
		if tagField.Tag("json") == fieldName {
			ok = true
			field = tagField
			return
		}
	}

	return
}

func (i *indexType) applyToMap(object map[string]interface{}) (contentToIndex [][]byte, ok bool) {
	var field interface{}
	for i, fieldName := range i.Selector {
		if i == 0 {
			field, ok = object[fieldName]
		} else {
			fieldMap, mapConvertionOk := field.(map[string]interface{})
			if !mapConvertionOk {
				return nil, false
			}
			field, ok = fieldMap[fieldName]
			if !ok {
				return nil, false
			}
		}
		if !ok {
			return nil, false
		}
	}
	return i.testType(field)
}

// doesFilterApplyToIndex only check if the filter belongs to the index
func (i *indexType) doesFilterApplyToIndex(filter *Filter) (ok bool) {
	// Check the selector
	if filter.selectorHash != i.SelectorHash {
		return false
	}

	// If at least one of the value has the right type the index need to be queried
	for _, value := range filter.values {
		if value.Type == i.Type {
			return true
		}
	}

	return false
}

func (i *indexType) testType(value interface{}) (contentToIndexes [][]byte, ok bool) {
	switch reflect.TypeOf(value).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(value)

		for j := 0; j < s.Len(); j++ {
			contentToIndex, ok := i.convertType(s.Index(j).Interface())
			if !ok {
				continue
			}

			contentToIndexes = append(contentToIndexes, contentToIndex)
		}
	default:
		contentToIndex, ok := i.convertType(value)
		if !ok {
			return nil, false
		}

		contentToIndexes = append(contentToIndexes, contentToIndex)
	}

	if len(contentToIndexes) <= 0 {
		return nil, false
	}

	return contentToIndexes, true
}

func (i *indexType) convertType(value interface{}) (contentToIndex []byte, ok bool) {
	var conversionFunc func(interface{}) ([]byte, error)
	switch i.Type {
	case StringIndex:
		conversionFunc = stringToBytes
	case IntIndex:
		conversionFunc = intToBytes
	case UIntIndex:
		conversionFunc = uintToBytes
	case TimeIndex:
		conversionFunc = timeToBytes
	default:
		return nil, false
	}

	var err error
	if contentToIndex, err = conversionFunc(value); err != nil {
		return nil, false
	}

	return contentToIndex, true
}

// query do the given filter and ad it to the tree
func (i *indexType) query(ctx context.Context, filter *Filter, finishedChan chan *idsType) {
	done := false
	defer func() {
		// Make sure to reply as done
		if !done && ctx.Err() == nil {
			finishedChan <- nil
			return
		}
	}()

	ids, _ := newIDs(ctx, filter.selectorHash, nil, nil)

	switch filter.GetType() {
	// If equal just this leave will be send
	case Equal:
		i.queryEqual(ctx, ids, filter)
	case Greater, Less:
		i.queryGreaterLess(ctx, ids, filter)
	case Between:
		i.queryBetween(ctx, ids, filter)
	}

	// Force to check first if a cancel signal has been send
	// If not already canceled it wait for done or cancel
	select {
	case <-ctx.Done():
		return
	default:
		select {
		case finishedChan <- ids:
		case <-ctx.Done():
			return
		}
	}

	done = true

	return
}

// newRefs builds a new empty Refs pointer
func newRefs() *refs {
	refs := new(refs)
	refs.Refs = []*ref{}
	return refs
}

// newRefsFromDB builds a Refs pointer based on the saved value in database
func newRefsFromDB(input []byte) *refs {
	refs := new(refs)
	json.Unmarshal(input, refs)
	return refs
}

// IDasBytes returns the ID of the coresponding object as a slice of bytes
func (r *refs) IDasBytes() []byte {
	return []byte(r.ObjectHashID)
}

// setIndexedValue add to the list of references this one.
// The indexName define the index it belongs to and indexedVal defines what value
// is indexed.
func (r *refs) setIndexedValue(indexName string, indexHash uint64, indexedVal []byte) {
	for _, ref := range r.Refs {
		if ref.IndexName == indexName {
			ref.IndexedValue = indexedVal
			return
		}
	}

	ref := new(ref)
	ref.IndexName = indexName
	ref.IndexHash = indexHash
	ref.IndexedValue = indexedVal
	r.Refs = append(r.Refs, ref)
}

// asBytes marshals the given Refs pointer into a slice of bytes fo saving
func (r *refs) asBytes() []byte {
	ret, _ := json.Marshal(r)
	return ret
}

// GetType returns the string representation of the index type
func (i *IndexInfo) GetType() string {
	switch i.Type {
	case StringIndex:
		return StringIndexString
	case IntIndex:
		return IntIndexString
	case UIntIndex:
		return UIntIndexString
	case TimeIndex:
		return TimeIndexString
	default:
		return ""
	}
}
