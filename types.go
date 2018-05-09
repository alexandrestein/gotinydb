package db

import (
	"github.com/emirpasic/gods/trees/btree"
)

const (
	blockSize      = 1024 * 1024 * 10 // 10MB
	filePermission = 0666             // u -> rw
)

type (
	DB struct {
		Collections map[string]*Collection
		path        string
	}

	Collection struct {
		Indexes map[string]*Index
		Meta    *Index
		path    string
	}

	Index struct {
		tree *btree.Tree
		path string
	}
)
