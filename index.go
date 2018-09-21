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
		// If this is a first level selector
		if j == 0 {
			field, ok = object.FieldOk(fieldName)
			// Check the JSON tag
			if !ok {
				field, ok = i.testJSONTag(object.Fields(), fieldName)
			}
		} else {
			var tmpField *structs.Field
			tmpField, ok = field.FieldOk(fieldName)
			// Check the JSON tag
			if !ok {
				field, ok = i.testJSONTag(field.Fields(), fieldName)
			} else {
				field = tmpField
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
	for _, fieldToTryWithJSONTags := range fields {
		JSONTag := fieldToTryWithJSONTags.Tag("json")
		if JSONTag == fieldName || JSONTag == fieldName+",omitempty" {
			ok = true
			field = fieldToTryWithJSONTags
			return
		}
	}

	return
}

func (i *indexType) selectorHash() uint16 {
	return buildSelectorHash(i.Selector)
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
	return i.testType(i.convertInterfaceValueFromMapToIndexType(field))
}

// doesFilterApplyToIndex only check if the filter belongs to the index
func (i *indexType) doesFilterApplyToIndex(filter *Filter) (ok bool) {
	// Check the selector
	if filter.selectorHash != i.selectorHash() {
		return false
	}

	// If at least one of the value has the right type the index need to be queried
	for _, value := range filter.values {
		if value.Type == i.Type {
			return true
		}
	}

	// Returns true to apply if the filter is a exists filter
	if len(filter.values) == 0 && filter.getType() == exists {
		return true
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
	ids, _ := newIDs(ctx, filter.selectorHash, nil, nil)

	switch filter.getType() {
	// If equal just this leave will be send
	case equal:
		i.queryEqual(ctx, ids, filter)
	case greater, less:
		i.queryGreaterLess(ctx, ids, filter)
	case between:
		i.queryBetween(ctx, ids, filter)
	case exists:
		i.queryExists(ctx, ids, filter)
	}

	// Force to check first if a cancel signal has been send
	// If not already canceled it wait for done or cancel
	select {
	case <-ctx.Done():
		return
	default:
	}

	finishedChan <- ids
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

// setIndexedValue add to the list of references this one.
// The indexName define the index it belongs to and indexedVal defines what value
// is indexed.
func (r *refs) setIndexedValue(indexName string, selectorHash uint16, indexedVal []byte) {
	// Looks into existing references
	for _, ref := range r.Refs {
		if ref.IndexName == indexName {
			ref.IndexedValue = indexedVal
			return
		}
	}

	// Build a new reference
	ref := new(ref)
	ref.IndexName = indexName
	ref.IndexedValue = indexedVal
	ref.IndexHash = selectorHash
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
		return string(StringIndexString)
	case IntIndex:
		return string(IntIndexString)
	case UIntIndex:
		return string(UIntIndexString)
	case TimeIndex:
		return string(TimeIndexString)
	default:
		return ""
	}
}
