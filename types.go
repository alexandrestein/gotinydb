package db

import (
	"github.com/emirpasic/gods/trees/btree"
)

const (
	blockSize = 1024 * 1024 * 50
)

type (
	Store struct {
		Path string
	}

	Collection struct {
		Indexes map[string]*Index
	}

	Index struct {
		tree *btree.Tree
		path string
	}
)
