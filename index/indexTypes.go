package index

import (
	"gitea.interlab-net.com/alexandre/db/query"
	"github.com/emirpasic/gods/trees/btree"
)

// Those constants defines the defferent types of indexes.
const (
	StringIndexType Type = iota
	IntIndexType
	CustomIndexType
)

// Those varables define the deferent errors
var (
	NotFoundString = "not found"
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
		selector  []string
		path      string
		indexType Type
	}

	// Type defines the type of indexing
	Type int

	// Index is the main interface of the package. it provides the functions to manage
	// those indexes
	Index interface {
		// Get returns the ID of the object corresponding to the given indexed value
		Get(indexedValue interface{}) (objectIDs []string, found bool)
		// GetNeighbours returns values interface and true if founded.
		// GetNeighbours(key interface{}, nBefore, nAfter int) (indexedValues []interface{}, objectIDs []string, found bool)
		// Put add the given value.
		Put(indexedValue interface{}, objectID string)
		// RemoveID clean the given value of the given id.
		RemoveID(value interface{}, objectID string) error
		// RemoveIDFromAll is a slow function. It will check any element of the tree to remove
		// all the entries where the given id is find.
		// The logic of the tree is in the opposit side so this function should only
		// be called for maintenance of the tree consistancy if any issue.
		RemoveIDFromAll(objectID string) error

		// Apply is the way the database knows if this index apply to the given
		// object. It traces the fields name with the selector statement.
		Apply(object interface{}) (valueToIndex interface{}, apply bool)

		// GetSelector return a list of strings. The selector is a list of
		// sub fields to track
		GetSelector() []string

		// RunQuery runs the given query and subqueries before returning the
		// corresponding ids.
		RunQuery(q *query.Query) (ids []string)

		// Save and Load saves or loads the tree at or from the path location from
		// the initialisation.
		Save() error
		Load() error

		GetAllIndexedValues() []interface{}
		GetAllIDs() []string

		getPath() string
		getTree() *btree.Tree

		Type() Type
	}
)
