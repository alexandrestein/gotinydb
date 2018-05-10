package db

import (
	"os"

	"github.com/emirpasic/gods/trees/btree"
)

const (
	blockSize      = 1024 * 1024 * 10 // 10MB
	filePermission = 0740             // u -> rwx | g -> r-- | o -> ---
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
		lockFile    *os.File
	}

	Collection struct {
		Indexes map[string]Index
		Meta    *MetaData
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
		selector  []interface{}
		path      string
		indexType IndexType
	}

	Record struct {
		Type RecordType
		Size int
		Data interface{}
	}

	IndexType  int
	RecordType int

	MetaData struct{}
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
