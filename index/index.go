package index

import (
	"fmt"
	"time"

	"github.com/alexandreStein/gods/trees/btree"
	"github.com/alexandreStein/gods/utils"
	"github.com/fatih/structs"
	"github.com/labstack/gommon/log"
)

func (i *structIndex) Get(indexedValue interface{}) ([]string, bool) {
	if indexedValue == nil {
		return nil, false
	}

	// Trys to get the list of ids for the given indexed value.
	idsAsInterface, found := i.tree.Get(indexedValue)
	if found {
		// Try to convert it into a slice of interface
		objectIDsAsInterface, okInterfaces := idsAsInterface.([]interface{})
		if okInterfaces {
			ret := []string{}
			// Loop all the interfaces to convert it into strings
			for _, objectIDAsInterface := range objectIDsAsInterface {
				objectIDsAsStrings, okStrings := objectIDAsInterface.(string)
				// Add the result to the return
				if okStrings {
					ret = append(ret, objectIDsAsStrings)
				}
			}

			// Returns a list of string corresponding to the object ids.
			return ret, found
		}

		objectIDsAsStrings, okStrings := idsAsInterface.([]string)
		if okStrings {
			return objectIDsAsStrings, found
		}
	}

	return nil, false
}

func (i *structIndex) Put(indexedValue interface{}, objectID string) {
	objectIDs, found := i.Get(indexedValue)
	if found {
		// Check that the given id is not allredy in the list
		for _, savedID := range objectIDs {
			if savedID == objectID {
				return
			}
		}
		objectIDs = append(objectIDs, objectID)
		i.tree.Put(indexedValue, objectIDs)
		return
	}

	i.tree.Put(indexedValue, []string{objectID})
}

func (i *structIndex) getName() string {
	return i.name
}

func (i *structIndex) getTree() *btree.Tree {
	return i.tree
}

func (i *structIndex) Type() utils.ComparatorType {
	return i.indexType
}

func (i *structIndex) Save() error {
	log.Print("SAVE")
	return nil

	// treeAsBytes, jsonErr := i.tree.ToJSON()
	// if jsonErr != nil {
	// 	return fmt.Errorf("durring JSON convertion: %s", jsonErr.Error())
	// }
	//
	// file, fileErr := os.OpenFile(i.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, vars.FilePermission)
	// if fileErr != nil {
	// 	return fmt.Errorf("opening file: %s", fileErr.Error())
	// }
	//
	// n, writeErr := file.WriteAt(treeAsBytes, 0)
	// if writeErr != nil {
	// 	return fmt.Errorf("writing file: %s", writeErr.Error())
	// }
	// if n != len(treeAsBytes) {
	// 	return fmt.Errorf("writes no complet, writen %d and have %d", len(treeAsBytes), n)
	// }
	//
	// return nil
}

func (i *structIndex) Load() error {
	log.Print("LOAD")
	return nil

	// file, fileErr := os.OpenFile(i.path, os.O_RDONLY, vars.FilePermission)
	// if fileErr != nil {
	// 	return fmt.Errorf("opening file: %s", fileErr.Error())
	// }
	//
	// buf := bytes.NewBuffer(nil)
	// at := int64(0)
	// for {
	// 	tmpBuf := make([]byte, vars.BlockSize)
	// 	n, readErr := file.ReadAt(tmpBuf, at)
	// 	if readErr != nil {
	// 		if io.EOF == readErr {
	// 			buf.Write(tmpBuf[:n])
	// 			break
	// 		}
	// 		return fmt.Errorf("%d readed but: %s", n, readErr.Error())
	// 	}
	// 	at = at + int64(n)
	// 	buf.Write(tmpBuf)
	// }
	//
	// err := i.tree.FromJSON(buf.Bytes())
	// if err != nil {
	// 	return fmt.Errorf("parsing block: %s", err.Error())
	// }
	//
	// return nil
}

func (i *structIndex) RemoveID(value interface{}, objectID string) error {
	savedIDs, found := i.Get(value)
	if !found {
		return fmt.Errorf(NotFoundString)
	}

	// newSavedIDs will take the updated list of ids
	newSavedIDs := []string{}
	for _, savedID := range savedIDs {
		if savedID != objectID {
			newSavedIDs = append(newSavedIDs, savedID)
		}
	}

	// If the new list of ids is empty the key value pair is delete
	if len(newSavedIDs) <= 0 {
		i.tree.Remove(value)
		return nil
	}

	// Save the ids if the size as changed
	if len(newSavedIDs) != len(savedIDs) {
		i.tree.Put(value, newSavedIDs)
	}

	return nil
}

func (i *structIndex) RemoveIDFromAll(id string) error {
	// Build new iterator at the start of the tree
	iter := i.tree.Iterator()

	// Loop every keys of the tree
	for iter.Next() {
		rmErr := i.RemoveID(iter.Key(), id)
		if rmErr != nil {
			if rmErr.Error() == NotFoundString {
				continue
			}
			return rmErr
		}
	}
	return nil
}

func (i *structIndex) doesApply(selector []string) bool {
	if len(selector) != len(i.GetSelector()) {
		return false
	}

	for j, savedField := range i.GetSelector() {
		if savedField != selector[j] {
			return false
		}
	}
	return true
}

func (i *structIndex) Apply(object interface{}) (valueToIndex interface{}, apply bool) {
	var mapObj map[string]interface{}
	if structs.IsStruct(object) {
		mapObj = structs.Map(object)
	} else if tmpMap, ok := object.(map[string]interface{}); ok {
		mapObj = tmpMap
	} else {
		return nil, false
	}

	var ok bool
	// Loop every level of the index
	for j, selectorElem := range i.selector {
		// If not at the top level
		if j < len(i.selector)-1 {
			// Trys to convert the value into map[string]interface{}
			mapObj, ok = mapObj[selectorElem].(map[string]interface{})
			// If not possible the index do not apply
			if !ok || mapObj == nil {
				return nil, false
			}
			// If at the top level
		} else {
			// Checks that the value is not nil
			if mapObj[selectorElem] == nil {
				return nil, false
			}

			// Check that the value in consustant with the specified index type
			// Convert to the coresponding type and check if the value is nil.
			switch i.Type() {
			case utils.StringComparatorType:
				if val, ok := mapObj[selectorElem].(string); !ok {
					return nil, false
				} else if val == "" {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.IntComparatorType:
				if val, ok := mapObj[selectorElem].(int); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.Int8ComparatorType:
				if val, ok := mapObj[selectorElem].(int8); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.Int16ComparatorType:
				if val, ok := mapObj[selectorElem].(int16); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.Int32ComparatorType:
				if val, ok := mapObj[selectorElem].(int32); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.Int64ComparatorType:
				if val, ok := mapObj[selectorElem].(int64); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.UIntComparatorType:
				if val, ok := mapObj[selectorElem].(uint); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.UInt8ComparatorType:
				if val, ok := mapObj[selectorElem].(uint8); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.UInt16ComparatorType:
				if val, ok := mapObj[selectorElem].(uint16); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.UInt32ComparatorType:
				if val, ok := mapObj[selectorElem].(uint32); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.UInt64ComparatorType:
				if val, ok := mapObj[selectorElem].(uint64); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.Float32ComparatorType:
				if val, ok := mapObj[selectorElem].(float32); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.Float64ComparatorType:
				if val, ok := mapObj[selectorElem].(float64); !ok {
					return nil, false
				} else if val == 0 {
					return nil, false
				} else {
					valueToIndex = val
				}
			case utils.TimeComparatorType:
				if val, ok := mapObj[selectorElem].(time.Time); !ok {
					return nil, false
				} else if val.IsZero() {
					return nil, false
				} else {
					valueToIndex = val
				}
			default:
				return nil, false
			}
		}
	}

	// Went to here, means that the index apply to this object
	return valueToIndex, true
}

func (i *structIndex) GetSelector() []string {
	return i.selector
}

func (i *structIndex) GetAllIndexedValues() (ret []interface{}) {
	iter := i.getTree().Iterator()
	for iter.Next() {
		ret = append(ret, iter.Key())
	}
	return
}

func (i *structIndex) GetAllIDs() (ret []string) {
	iter := i.getTree().Iterator()
	for iter.Next() {
		ret = append(ret, iter.Value().([]string)...)
	}
	return
}
