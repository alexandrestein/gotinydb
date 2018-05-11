package index

import (
	"github.com/emirpasic/gods/trees/btree"
)

const (
	StringIndexType IndexType = iota
	IntIndexType
	CustomIndexType
)

type (
	StringIndex struct {
		*StructIndex
	}

	IntIndex struct {
		*StructIndex
	}

	StructIndex struct {
		tree      *btree.Tree
		selector  []interface{}
		path      string
		indexType IndexType
	}

	IndexType int

	Index interface {
		Get(indexedValue interface{}) (objectID string, found bool)
		GetNeighbours(key interface{}, nBefore, nAfter int) (indexedValues []interface{}, objectIDs []string, found bool)
		Put(indexedValue interface{}, objectID string)
		RemoveId(objectID string) error

		Save() error
		Load() error

		getPath() string
		getTree() *btree.Tree

		Type() IndexType
	}
)
