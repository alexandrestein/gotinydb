package collection

import (
	"context"
	"fmt"
	"time"

	"github.com/alexandreStein/gods/trees/btree"
	"github.com/alexandreStein/gods/utils"

	"github.com/fatih/structs"
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

func (i *structIndex) Save() ([]byte, error) {
	treeAsBytes, jsonErr := i.tree.ToJSON()
	if jsonErr != nil {
		return nil, fmt.Errorf("durring JSON convertion: %s", jsonErr.Error())
	}

	return treeAsBytes, nil
}

func (i *structIndex) Load(content []byte) error {
	err := i.tree.FromJSON(content)
	if err != nil {
		return fmt.Errorf("parsing block: %s", err.Error())
	}

	return nil
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

// func (i *structIndex) RunQuery(q *query.Query) (ids []string) {
// 	// if q == nil {
// 	// 	return
// 	// }
// 	//
// 	// ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
// 	// defer cancel()
// 	//
// 	// getIDs := []string{}
// 	// keepIDs := []string{}
// 	// getIDsChan := i.buildIDList(ctx, q.GetActions)
// 	// keepIDsChan := i.buildIDList(ctx, q.KeepActions)
// 	//
// 	// select {
// 	// case retIDs := <-getIDsChan:
// 	// 	getIDs = retIDs
// 	// case retIDs := <-keepIDsChan:
// 	// 	keepIDs = retIDs
// 	// case <-ctx.Done():
// 	// 	return
// 	// }
//
// 	// // Clean the retreived IDs of the keep selection
// 	// for j := len(getIDs) - 1; j <= 0; j-- {
// 	// 	for _, keepID := range keepIDs {
// 	// 		if getIDs[j] == keepID {
// 	// 			getIDs = append(getIDs[:j], getIDs[j+1:]...)
// 	// 		}
// 	// 	}
// 	// 	if q.Distinct {
// 	// 		keys := make(map[string]bool)
// 	// 		list := []string{}
// 	// 		if _, value := keys[getIDs[j]]; !value {
// 	// 			keys[getIDs[j]] = true
// 	// 			list = append(list, getIDs[j])
// 	// 		}
// 	// 		ids = list
// 	// 	}
// 	// }
// 	//
// 	// // Do the limit
// 	// ids = getIDs[:q.Limit]
//
// 	// // Actualy run the query
// 	// ids = i.runQuery(q)
// 	// if q.KeepAction != nil && len(ids) != 0 {
// 	// 	ids = i.runKeepQuery(q, ids)
// 	// }
// 	//
// 	// if q.Distinct {
// 	// 	keys := make(map[string]bool)
// 	// 	list := []string{}
// 	// 	for _, id := range ids {
// 	// 		if _, value := keys[id]; !value {
// 	// 			keys[id] = true
// 	// 			list = append(list, id)
// 	// 		}
// 	// 	}
// 	// }
// 	//
// 	// // Cleans the list if to big and returns
// 	// if len(ids) > q.Limit {
// 	// 	ids = ids[:q.Limit]
// 	// 	return
// 	// }
// 	//
// 	// // Reverts the result if wanted
// 	// if q.InvertedOrder {
// 	// 	for i := len(ids)/2 - 1; i >= 0; i-- {
// 	// 		opp := len(ids) - 1 - i
// 	// 		ids[i], ids[opp] = ids[opp], ids[i]
// 	// 	}
// 	// }
// 	return
// }

func (i *structIndex) RunQuery(ctx context.Context, actions []*Action, retChan chan []string) {
	responseChan := make(chan []string, 16)
	defer close(retChan)
	defer close(responseChan)

	if len(actions) == 0 {
		return
	}

	nbToWait := 0
	for _, action := range actions {
		if !i.doesApply(action.Selector) {
			continue
		}

		go getIDs(ctx, i, action, responseChan)
		nbToWait++
	}

	ret := []string{}

	for {
		select {
		case ids := <-responseChan:
			ret = append(ret, ids...)
			retChan <- ret
		case <-ctx.Done():
			return
		}
		nbToWait--
		if nbToWait <= 0 {
			return
		}
	}
}

func getIDs(ctx context.Context, i *structIndex, action *Action, responseChan chan []string) {
	ids := i.runQuery(action)
	responseChan <- ids
}

func (i *structIndex) runQuery(action *Action) (ids []string) {
	// If equal just this leave will be send
	if action.GetType() == Equal {
		tmpIDs, found := i.Get(action.CompareToValue)
		if found {
			ids = tmpIDs
		}
		return
	}

	var iterator btree.Iterator
	var iteratorInit bool
	var nextFunc (func() bool)
	var keyFound bool

	if action.GetType() == Greater {
		_, keyAfter, found := i.tree.GetClosestKeys(action.CompareToValue)
		keyFound = found
		if keyAfter != nil {
			iterator, _ = i.tree.IteratorAt(keyAfter)
			iteratorInit = true
		}
		nextFunc = iterator.Next
	} else if action.GetType() == Less {
		keyBefore, _, found := i.tree.GetClosestKeys(action.CompareToValue)
		keyFound = found
		if keyBefore != nil {
			iterator, _ = i.tree.IteratorAt(keyBefore)
			iteratorInit = true
		}
		nextFunc = iterator.Prev
	}

	// Check if the caller want more or less with equal option
	if keyFound {
		if !action.KeepEqual {
			ids = append(ids, iterator.Value().([]string)...)
		}
	} else {
		if iteratorInit {
			ids = append(ids, iterator.Value().([]string)...)
		}
	}

	if !iteratorInit {
		return
	}

	for nextFunc() {
		ids = append(ids, iterator.Value().([]string)...)
	}

	return
}

// func (i *structIndex) Apply(object interface{}) (valueToIndex interface{}, apply bool) {
// 	var mapObj map[string]interface{}
// 	if structs.IsStruct(object) {
// 		mapObj = structs.Map(object)
// 	} else if tmpMap, ok := object.(map[string]interface{}); ok {
// 		mapObj = tmpMap
// 	} else {
// 		return nil, false
// 	}
//
// 	var ok bool
// 	// Loop every level of the index
// 	for j, selectorElem := range i.selector {
// 		// If not at the top level
// 		if j < len(i.selector)-1 {
// 			// Trys to convert the value into map[string]interface{}
// 			mapObj, ok = mapObj[selectorElem].(map[string]interface{})
// 			// If not possible the index do not apply
// 			if !ok || mapObj == nil {
// 				return nil, false
// 			}
// 			// If at the top level
// 		} else {
// 			// Checks that the value is not nil
// 			if mapObj[selectorElem] == nil {
// 				return nil, false
// 			}
//
// 			// Check that the value in consustant with the specified index type
// 			// Convert to the coresponding type and check if the value is nil.
// 			switch i.Type() {
// 			case StringIndexType:
// 				if val, ok := mapObj[selectorElem].(string); !ok {
// 					return nil, false
// 				} else if val == "" {
// 					return nil, false
// 				} else {
// 					valueToIndex = val
// 				}
// 			case IntIndexType:
// 				if val, ok := mapObj[selectorElem].(int); !ok {
// 					return nil, false
// 				} else if val == 0 {
// 					return nil, false
// 				} else {
// 					valueToIndex = val
// 				}
// 			default:
// 				return nil, false
// 			}
// 		}
// 	}
//
// 	// Went to here, means that the index apply to this object
// 	return valueToIndex, true
// }

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
