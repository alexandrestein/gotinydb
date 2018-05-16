package index

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"gitea.interlab-net.com/alexandre/db/vars"
	"github.com/emirpasic/gods/trees/btree"
)

func NewStringIndex(path string) *StringIndex {
	i := &StringIndex{
		NewStructIndex(path),
	}
	i.tree = btree.NewWithStringComparator(vars.TreeOrder)
	i.indexType = StringIndexType

	return i
}

func NewIntIndex(path string) *IntIndex {
	i := &IntIndex{
		NewStructIndex(path),
	}
	i.tree = btree.NewWithIntComparator(vars.TreeOrder)
	i.indexType = IntIndexType

	return i
}

func NewStructIndex(path string) *StructIndex {
	return &StructIndex{
		path: path,
	}
}

func (i *StructIndex) Get(indexedValue interface{}) (string, bool) {
	idAsInterface, found := i.tree.Get(indexedValue)
	objectID, ok := idAsInterface.(string)
	if !ok {
		return "", false
	}

	return objectID, found
}
func (i *StructIndex) Put(indexedValue interface{}, objectID string) {
	i.tree.Put(indexedValue, objectID)
}

// GetNeighbours returns values interface and true if founded.
func (i *StructIndex) GetNeighbours(key interface{}, nBefore, nAfter int) (indexedValues []interface{}, objectIDs []string, found bool) {
	iterator := i.tree.IteratorAt(key)
	// iterator2 := *iterator

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

		idAsInterface, ok := iterator.Value().(string)
		// If the values is not an object ID it is not append.
		if !ok {
			continue
		}

		indexedValues = append(indexedValues, iterator.Key())
		objectIDs = append(objectIDs, idAsInterface)

	}
	return
}

func (i *StructIndex) getPath() string {
	return i.path
}

func (i *StructIndex) getTree() *btree.Tree {
	return i.tree
}

func (i *StructIndex) Type() IndexType {
	return i.indexType
}

func (i *StructIndex) Save() error {
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

func (i *StructIndex) Load() error {
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

func (i *StructIndex) RemoveId(id string) error {
	iter := i.tree.Iterator()

	for iter.Next() {
		savedID, ok := iter.Value().(string)
		if !ok {
			continue
		}

		if savedID == id {
			i.tree.Remove(iter.Key())
		}
	}

	return nil
}

func (i *StructIndex) Update(oldValue, newValue interface{}, id string) error {
	return nil
}
