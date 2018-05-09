package db

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/emirpasic/gods/trees/btree"
)

func NewStringIndex(path string) *StringIndex {
	i := &StringIndex{
		NewStructIndex(path),
	}
	i.tree = btree.NewWithStringComparator(treeOrder)
	i.indexType = StringIndexType

	return i
}

// func NewIntIndex(path string) *IntIndex {
// 	i := &IntIndex{
// 		NewStructIndex(path),
// 	}
// 	i.tree = btree.NewWithIntComparator(treeOrder)
// 	i.indexType = IntIndexType
//
// 	return i
// }

func NewStructIndex(path string) *StructIndex {
	return &StructIndex{
		path: path,
	}
}

func (i *StructIndex) Get(key interface{}) (interface{}, bool) {
	return i.tree.Get(key)
}
func (i *StructIndex) Put(key interface{}, value interface{}) {
	i.tree.Put(key, value)
}

func (i *StructIndex) GetPath() string {
	return i.path
}

func (i *StructIndex) GetTree() *btree.Tree {
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

	file, fileErr := os.OpenFile(i.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePermission)
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
	file, fileErr := os.OpenFile(i.path, os.O_RDONLY, filePermission)
	if fileErr != nil {
		return fmt.Errorf("opening file: %s", fileErr.Error())
	}

	buf := bytes.NewBuffer(nil)
	at := int64(0)
	for {
		tmpBuf := make([]byte, blockSize)
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
