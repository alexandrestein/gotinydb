package gotinydb

import (
	"context"

	"github.com/alexandreStein/gods/trees/btree"
	"github.com/alexandreStein/gods/utils"
	bolt "github.com/coreos/bbolt"
)

type (
	// DB is the main element of the package it defines the all database.
	DB struct {
		collections map[string]*Collection
		boltDB      *bolt.DB
		path        string
	}

	// Collection define the main element of the database. This is where data are
	// stored. The design is similar to other NO-SQL database.
	Collection struct {
		Name    string
		Indexes map[string]Index

		boltDB *bolt.DB
	}

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
		RunQuery(ctx context.Context, actions []*Action, retChan chan []string)

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

	// Query defines the object to request index query.
	Query struct {
		GetActions, KeepActions []*Action

		// OrderBy       []string
		InvertedOrder bool

		Limit int

		Distinct bool
	}

	// Action defines the way the query will be performed
	Action struct {
		Selector       []string
		Operation      ActionType
		CompareToValue interface{}
		KeepEqual      bool
	}

	// ActionType defines the type of action to perform.
	ActionType string
)
