package index

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"gitea.interlab-net.com/alexandre/db/vars"
	"github.com/emirpasic/gods/trees/btree"
)

// NewStringIndex returns Index interface ready to manage string types
func NewStringIndex(path string) Index {
	i := &stringIndex{
		newStructIndex(path),
	}
	i.tree = btree.NewWithStringComparator(vars.TreeOrder)
	i.indexType = StringIndexType

	return i
}

// NewIntIndex returns Index interface ready to manage int types
func NewIntIndex(path string) Index {
	i := &intIndex{
		newStructIndex(path),
	}
	i.tree = btree.NewWithIntComparator(vars.TreeOrder)
	i.indexType = IntIndexType

	return i
}

func newStructIndex(path string) *structIndex {
	return &structIndex{
		path: path,
	}
}

func (i *structIndex) Get(indexedValue interface{}) ([]string, bool) {
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

// GetNeighbours returns values interface and true if founded.
func (i *structIndex) GetNeighbours(key interface{}, nBefore, nAfter int) (indexedValues []interface{}, objectIDs []string, found bool) {
	iterator := i.tree.IteratorAt(key)

	nToAdd := 0

	if iterator.Key() == key {
		found = true
		nToAdd++
	}

	// Go to the right place
	for i := 0; i <= nBefore; i++ {
		if !iterator.Prev() {
			nBefore = i
			break
		}
	}

	for i := 0; i < nToAdd+nBefore+nAfter; i++ {
		if !iterator.Next() {
			break
		}

		idAsInterface, ok := iterator.Value().([]string)
		// If the values is not an object ID it is not append.
		if !ok {
			continue
		}

		indexedValues = append(indexedValues, iterator.Key())
		objectIDs = append(objectIDs, idAsInterface...)

	}
	return
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

func (i *structIndex) Update(oldValue, newValue interface{}, id string) error {
	rmErr := i.RemoveID(oldValue, id)
	if rmErr != nil {
		return fmt.Errorf("trying to delete the ID: %q from the given value %v: %s",
			id, oldValue, rmErr.Error(),
		)
	}

	i.Put(newValue, id)
	return nil
}

func (i *structIndex) Query(q *Query) *Query {
	return nil
}

func (i *structIndex) RunQuery(q *Query) (ids []string) {
	return ids
}
