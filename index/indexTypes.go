package index

import (
	"context"

	"github.com/alexandreStein/GoTinyDB/query"
	"github.com/alexandreStein/gods/trees/btree"
	"github.com/alexandreStein/gods/utils"
)

// // Those constants defines the defferent types of indexes.
// const (
// 	StringIndexType Type = iota
// 	IntIndexType
// 	CustomIndexType
// )

// Those varables define the deferent errors
var (
	NotFoundString = "not found"
)

type (
	structIndex struct {
		tree      *btree.Tree
		selector  []string
		name      string
		indexType utils.ComparatorType
	}

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

		// Apply is the way the database knows if this index apply to the given
		// object. It traces the fields name with the selector statement.
		Apply(object interface{}) (valueToIndex interface{}, apply bool)

		// GetSelector return a list of strings. The selector is a list of
		// sub fields to track
		GetSelector() []string

		// RunQuery runs the given query and subqueries before returning the
		// corresponding ids.
		RunQuery(ctx context.Context, actions []*query.Action, retChan chan []string)

		// Save and Load saves or loads the tree at or from the path location from
		// the initialisation.
		Save() ([]byte, error)
		Load(content []byte) error

		GetAllIndexedValues() []interface{}
		GetAllIDs() []string

		getName() string
		getTree() *btree.Tree

		Type() utils.ComparatorType
	}
)
