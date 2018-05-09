package db

import (
	"github.com/emirpasic/gods/trees/btree"
)

type (
	DB struct {
		Store *Store
	}

	Store struct {
		Blocks []*Block
	}

	Block struct {
		Path string
		Tree *btree.Tree
	}
)
