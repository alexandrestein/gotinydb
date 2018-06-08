package gotinydb

import (
	"bytes"

	"github.com/google/btree"
)

// Thoses constants defines the different types of action to perform at query
const (
	Equal   ActionType = "eq"
	Greater ActionType = "gr"
	Less    ActionType = "le"
)

type (
	// Query defines the object to request index query.
	Query struct {
		getActions, cleanActions []*Action

		orderBy       []string
		revertedOrder bool

		limit int

		distinct bool
	}

	// Action defines the way the query will be performed
	Action struct {
		selector       []string
		operation      ActionType
		compareToValue interface{}
		equal          bool

		limit int
	}

	// IDs is a type to manage IDs during query to be compatible with the tree query
	IDs struct {
		indexedValue []byte
		idsAsByte    []byte
	}

	queryResponse struct {
		IDs []*IDs
	}

	// ActionType defines the type of action to perform.
	ActionType string
)

// NewQuery build a new query object.
// It also set the default limit to 1000.
func NewQuery() *Query {
	return &Query{
		limit: 1000,
	}
}

// SetLimit defines the configurable limit of IDs.
func (q *Query) SetLimit(l int) *Query {
	q.limit = l
	for _, action := range q.getActions {
		action.limit = l
	}
	for _, action := range q.cleanActions {
		action.limit = l
	}
	return q
}

// // InvertOrder lets the caller invert the slice if wanted.
// func (q *Query) InvertOrder() *Query {
// 	q.InvertedOrder = true
// 	return q
// }

// DistinctWanted clean the duplicated IDs
func (q *Query) DistinctWanted() *Query {
	q.distinct = true
	return q
}

// Get defines the action to perform to get IDs
func (q *Query) Get(a *Action) *Query {
	if q.getActions == nil {
		q.getActions = []*Action{}
	}
	a.limit = q.limit
	q.getActions = append(q.getActions, a)
	return q
}

// Clean defines the actions to perform to clean IDs which have already retrieved
// by the Get actions.
func (q *Query) Clean(a *Action) *Query {
	if q.cleanActions == nil {
		q.getActions = []*Action{a}
	}
	a.limit = q.limit
	q.cleanActions = append(q.cleanActions, a)
	return q
}

// NewIDs build a new Ids pointer from a slice of bytes
func NewIDs(idsAsBytes []byte) *IDs {
	ids := new(IDs)
	ids.indexedValue = idsAsBytes

	return ids
}

// Less must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b
func (i *IDs) Less(compareToItem btree.Item) bool {
	compareTo, ok := compareToItem.(*IDs)
	if !ok {
		return false
	}

	n := bytes.Compare(i.indexedValue, compareTo.indexedValue)
	if n < 0 {
		return true
	}
	return false
}

func (i *IDs) treeItem() btree.Item {
	return btree.Item(i)
}

func iterator(maxResponse int) (func(next btree.Item) (over bool), *queryResponse) {
	ret := new(queryResponse)
	ret.IDs = make([]*IDs, 0)

	return func(next btree.Item) bool {
		if len(ret.IDs) >= maxResponse {
			return false
		}

		nextAsIDs, ok := next.(*IDs)
		if !ok {
			return false
		}

		ret.IDs = append(ret.IDs, nextAsIDs)
		return true
	}, ret
}
