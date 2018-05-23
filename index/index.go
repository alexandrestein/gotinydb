package index

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"gitea.interlab-net.com/alexandre/db/query"
	"gitea.interlab-net.com/alexandre/db/vars"
	"github.com/emirpasic/gods/trees/btree"
	"github.com/fatih/structs"
)

// NewStringIndex returns Index interface ready to manage string types
func NewStringIndex(path string, selector []string) Index {
	i := &stringIndex{
		newStructIndex(path, selector),
	}
	i.tree = btree.NewWithStringComparator(vars.TreeOrder)
	i.indexType = StringIndexType

	return i
}

// NewIntIndex returns Index interface ready to manage int types
func NewIntIndex(path string, selector []string) Index {
	i := &intIndex{
		newStructIndex(path, selector),
	}
	i.tree = btree.NewWithIntComparator(vars.TreeOrder)
	i.indexType = IntIndexType

	return i
}

func newStructIndex(path string, selector []string) *structIndex {
	return &structIndex{
		path:     path,
		selector: selector,
	}
}

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

func (i *structIndex) getPath() string {
	return i.path
}

func (i *structIndex) getTree() *btree.Tree {
	return i.tree
}

func (i *structIndex) Type() Type {
	return i.indexType
}

func (i *structIndex) Save() error {
	treeAsBytes, jsonErr := i.tree.ToJSON()
	if jsonErr != nil {
		return fmt.Errorf("durring JSON convertion: %s", jsonErr.Error())
	}

	file, fileErr := os.OpenFile(i.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, vars.FilePermission)
	if fileErr != nil {
		return fmt.Errorf("opening file: %s", fileErr.Error())
	}

	n, writeErr := file.WriteAt(treeAsBytes, 0)
	if writeErr != nil {
		return fmt.Errorf("writing file: %s", writeErr.Error())
	}
	if n != len(treeAsBytes) {
		return fmt.Errorf("writes no complet, writen %d and have %d", len(treeAsBytes), n)
	}

	return nil
}

func (i *structIndex) Load() error {
	file, fileErr := os.OpenFile(i.path, os.O_RDONLY, vars.FilePermission)
	if fileErr != nil {
		return fmt.Errorf("opening file: %s", fileErr.Error())
	}

	buf := bytes.NewBuffer(nil)
	at := int64(0)
	for {
		tmpBuf := make([]byte, vars.BlockSize)
		n, readErr := file.ReadAt(tmpBuf, at)
		if readErr != nil {
			if io.EOF == readErr {
				buf.Write(tmpBuf[:n])
				break
			}
			return fmt.Errorf("%d readed but: %s", n, readErr.Error())
		}
		at = at + int64(n)
		buf.Write(tmpBuf)
	}

	err := i.tree.FromJSON(buf.Bytes())
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

func (i *structIndex) RunQuery(q *query.Query) (ids []string) {
	if q == nil {
		fmt.Println("check selecotr", i.GetSelector(), q.Selector)
		return
	}

	if !i.doesApply(q.Selector) {
		return
	}

	// Actualy run the query
	ids = i.runQuery(q, true)
	if q.KeepAction != nil && len(ids) != 0 {
		ids = i.runKeepQuery(q, ids)
	}

	if q.Distinct {
		keys := make(map[string]bool)
		list := []string{}
		for _, id := range ids {
			if _, value := keys[id]; !value {
				keys[id] = true
				list = append(list, id)
			}
		}
	}

	// Cleans the list if to big and returns
	if len(ids) > q.Limit {
		ids = ids[:q.Limit]
		return
	}

	// Reverts the result if wanted
	if q.InvertedOrder {
		for i := len(ids)/2 - 1; i >= 0; i-- {
			opp := len(ids) - 1 - i
			ids[i], ids[opp] = ids[opp], ids[i]
		}
	}
	return
}

func (i *structIndex) runQuery(q *query.Query, get bool) (ids []string) {
	var action *query.Action

	// If query is not nil
	if q == nil {
		return
	}

	// If the caller want to do the get lockup
	if get {
		// If the get action is nil it returns
		if q.GetAction == nil {
			return
		}
		// Otherways it save the action
		action = q.GetAction
		// The caller do not asked for a get action and the saved action is keep
		// if any
	} else {
		if q.KeepAction == nil {
			return
		}
		action = q.KeepAction
	}

	if action.CompareToValue == nil {
		return
	}

	// // Check the selector
	// if !reflect.DeepEqual(q.Selector, i.selector) {
	// 	return
	// }

	// If equal just this leave will be send
	if action.GetType() == query.Equal {
		tmpIDs, found := i.Get(action.CompareToValue)
		if found {
			ids = tmpIDs
			if len(ids) > q.Limit {
				ids = ids[:q.Limit]
			}
		}
		return
	}

	var iterator btree.Iterator
	var iteratorInit bool
	var nextFunc (func() bool)
	var keyFound bool

	if action.GetType() == query.Greater {
		_, keyAfter, found := i.tree.GetClosestKeys(action.CompareToValue)
		keyFound = found
		if keyAfter != nil {
			iterator, _ = i.tree.IteratorAt(keyAfter)
			iteratorInit = true
		}
		nextFunc = iterator.Next
	} else if action.GetType() == query.Less {
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
		if !q.KeepEqual {
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

func (i *structIndex) runKeepQuery(q *query.Query, ids []string) []string {
	// Do the lock up for the IDs to remove
	idsToRemove := i.runQuery(q, false)
	indexToRemove := []int{}

	// Do the check of which IDs need to be removed
	for j, id1 := range ids {
		for _, id2 := range idsToRemove {
			if id1 == id2 {
				// Build the list of ids to remove
				indexToRemove = append(indexToRemove, j)
			}
		}
	}

	// Once we know which index we need to remove we start at the end of the slice
	// and we remove every not needed IDs.
	for j := len(indexToRemove) - 1; j >= 0; j-- {
		if len(ids) > indexToRemove[j] {
			ids = append(ids[:indexToRemove[j]], ids[indexToRemove[j]+1:]...)
		} else {
			ids = ids[:indexToRemove[j]]
		}
	}

	return ids
}

func (i *structIndex) Apply(object interface{}) (valueToIndex interface{}, apply bool) {
	if structs.IsStruct(object) {
		mapObj := structs.Map(object)
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
				case StringIndexType:
					if val, ok := mapObj[selectorElem].(string); !ok {
						return nil, false
					} else if val == "" {
						return nil, false
					} else {
						valueToIndex = val
					}
				case IntIndexType:
					if val, ok := mapObj[selectorElem].(int); !ok {
						return nil, false
					} else if val == 0 {
						return nil, false
					} else {
						valueToIndex = val
					}
				}
			}
		}
	}

	// Went to here, means that the index apply to this object
	return valueToIndex, true
}

func (i *structIndex) GetSelector() []string {
	return i.selector
}
