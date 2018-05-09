package db

import (
	"github.com/emirpasic/gods/trees/btree"
)

const (
	blockSize      = 1024 * 1024 * 10 // 10MB
	filePermission = 0666             // u -> rw
	treeOrder      = 10
)

const (
	StringIndexType IndexType = iota
	IntIndexType
	CustomIndexType
)

type (
	DB struct {
		Collections map[string]*Collection
		path        string
	}

	Collection struct {
		Indexes map[string]Index
		Meta    *Index
		path    string
	}

	StringIndex struct {
		*StructIndex
	}

	IntIndex struct {
		*StructIndex
	}

	StructIndex struct {
		tree      *btree.Tree
		path      string
		indexType IndexType
	}

	IndexType int
)

type (
	Index interface {
		Get(interface{}) (interface{}, bool)
		Put(interface{}, interface{})

		Save() error
		Load() error

		GetPath() string
		GetTree() *btree.Tree

		Type() IndexType
	}
)
