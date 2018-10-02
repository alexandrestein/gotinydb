package gotinydb

import (
	"github.com/fatih/structs"
)

// apply take the full object to add in the collection and check if is must be
// indexed or not. If the object needs to be indexed the value to index is returned as a byte slice.
func (s selector) apply(object interface{}) (contentToIndex interface{}, ok bool) {
	if structs.IsStruct(object) {
		return s.applyToStruct(structs.New(object))
	}

	if mp, ok := object.(map[string]interface{}); ok {
		return s.applyToMap(mp)
	}

	return nil, false
}

func (s selector) applyToStruct(object *structs.Struct) (contentToIndex interface{}, ok bool) {
	var field *structs.Field
	for j, fieldName := range s {
		// If this is a first level selector
		if j == 0 {
			field, ok = object.FieldOk(fieldName)
			// Check the JSON tag
			if !ok {
				field, ok = s.testJSONTag(object.Fields(), fieldName)
			}
		} else {
			var tmpField *structs.Field
			tmpField, ok = field.FieldOk(fieldName)
			// Check the JSON tag
			if !ok {
				field, ok = s.testJSONTag(field.Fields(), fieldName)
			} else {
				field = tmpField
			}
		}

		if !ok {
			// return i.testJSONTag(object)
			return nil, false
		}
	}

	return field.Value(), true
}

func (s selector) testJSONTag(fields []*structs.Field, fieldName string) (field *structs.Field, ok bool) {
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

func (s selector) selectorHash() uint16 {
	return buildSelectorHash(s)
}

func (s selector) applyToMap(object map[string]interface{}) (contentToIndex interface{}, ok bool) {
	var field interface{}
	for i, fieldName := range s {
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
	return field, true
}

// func (s *selector) testType(value interface{}) (contentToIndexes [][]byte, ok bool) {
// 	switch reflect.TypeOf(value).Kind() {
// 	case reflect.Slice:
// 		s := reflect.ValueOf(value)

// 		for j := 0; j < s.Len(); j++ {
// 			contentToIndex, ok := i.convertType(s.Index(j).Interface())
// 			if !ok {
// 				continue
// 			}

// 			contentToIndexes = append(contentToIndexes, contentToIndex)
// 		}
// 	default:
// 		contentToIndex, ok := i.convertType(value)
// 		if !ok {
// 			return nil, false
// 		}

// 		contentToIndexes = append(contentToIndexes, contentToIndex)
// 	}

// 	if len(contentToIndexes) <= 0 {
// 		return nil, false
// 	}

// 	return contentToIndexes, true
// }

// func (s *selector) convertType(value interface{}) (contentToIndex []byte, ok bool) {
// 	var conversionFunc func(interface{}) ([]byte, error)
// 	switch i.Type {
// 	case StringIndex:
// 		conversionFunc = stringToBytes
// 	case IntIndex:
// 		conversionFunc = intToBytes
// 	case UIntIndex:
// 		conversionFunc = uintToBytes
// 	case TimeIndex:
// 		conversionFunc = timeToBytes
// 	default:
// 		return nil, false
// 	}

// 	var err error
// 	if contentToIndex, err = conversionFunc(value); err != nil {
// 		return nil, false
// 	}

// 	return contentToIndex, true
// }
