package index

import (
	"github.com/emirpasic/gods/trees/btree"
)

// Those constants defines the defferent types of indexes.
const (
	StringIndexType Type = iota
	IntIndexType
	CustomIndexType
)

type (
	stringIndex struct {
		*structIndex
	}

	intIndex struct {
		*structIndex
	}

	structIndex struct {
		tree      *btree.Tree
		selector  []interface{}
		path      string
		indexType Type
	}

	// Type defines the type of indexing
	Type int

	// Index is the main interface of the package. it provides the functions to manage
	// those indexes
	Index interface {
		// Get returns the ID of the object corresponding to the given indexed value
		Get(indexedValue interface{}) (objectID string, found bool)
		// GetNeighbours returns values interface and true if founded.
		GetNeighbours(key interface{}, nBefore, nAfter int) (indexedValues []interface{}, objectIDs []string, found bool)
		// Put add the given value.
		Put(indexedValue interface{}, objectID string)
		// RemoveID is a slow function. It will check any element of the tree to remove
		// all the entries where the given id is find.
		// The logic of the tree is in the opposit side so this function should only
		// be called for maintenance of the tree consistancy if any issue.
		RemoveID(objectID string) error
		// This is the way the tree should be manage consistancy.
		// Any time a intry is update this function should be called to mange the
		// index.
		Update(oldValue, newValue interface{}, id string) error

		// Save and Load saves or loads the tree at or from the path location from
		// the initialisation.
		Save() error
		Load() error

		getPath() string
		getTree() *btree.Tree

		Type() Type
	}
)
