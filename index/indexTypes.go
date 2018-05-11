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
		Get(interface{}) (interface{}, bool)
		Put(interface{}, interface{})

		Save() error
		Load() error

		getPath() string
		getTree() *btree.Tree

		Type() IndexType
	}
)
