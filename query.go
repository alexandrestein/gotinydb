package gotinydb

import (
	"encoding/json"

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

	// ID is a type to order IDs during query to be compatible with the tree query
	ID string

	// IDs defines a list of ID. The struct is needed to build a pointer to be
	// passed to deferent functions
	IDs struct {
		Slice []*ID
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
func NewIDs(idsAsBytes []byte) (*IDs, error) {
	// func NewIDs(idsAsBytes []byte) ([]*ID, error) {
	ids := new(IDs)

	err := json.Unmarshal(idsAsBytes, ids)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// Less must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b
func (i *ID) Less(compareToItem btree.Item) bool {
	compareTo, ok := compareToItem.(*ID)
	if !ok {
		return false
	}

	return (*i < *compareTo)
}

func (i *ID) treeItem() btree.Item {
	return btree.Item(i)
}

func (i *ID) String() string {
	return string(*i)
}

func iterator(maxResponse int) (func(next btree.Item) (over bool), *IDs) {
	ret := new(IDs)

	return func(next btree.Item) bool {
		if len(ret.Slice) >= maxResponse {
			return false
		}

		nextAsID, ok := next.(*ID)
		if !ok {
			return false
		}

		ret.Slice = append(ret.Slice, nextAsID)
		return true
	}, ret
}

// func iteratorIntoStringSlice(targetSlice []string, maxResponse int) func(next btree.Item) (over bool) {
// 	return func(next btree.Item) bool {
// 		if len(targetSlice) >= maxResponse {
// 			return false
// 		}

// 		nextAsID, ok := next.(*ID)
// 		if !ok {
// 			return false
// 		}

// 		targetSlice = append(targetSlice, nextAsID.String())

// 		return true
// 	}
// }

// func cleanIterator(tree *btree.BTree) func(next btree.Item) (over bool) {
// 	return func(next btree.Item) bool {
// 		if len(ret.IDs) >= maxResponse {
// 			return false
// 		}

// 		nextAsIDs, ok := next.(*IDs)
// 		if !ok {
// 			return false
// 		}

// 		idsAsString := nextAsIDs.getIDsAsStrings()
// 		if idsAsString == nil {
// 			return false
// 		}

// 		ret.IDs = append(ret.IDs, nextAsIDs)
// 		return true
// 	}
// }

func (i *IDs) SetID(idToSet string) {
	id := ID(idToSet)
	i.Slice = append(i.Slice, &id)
}

func (i *IDs) RmID(idToRm string) {
	for j, id := range i.Slice {
		if id.String() == idToRm {
			copy(i.Slice[j:], i.Slice[j+1:])
			i.Slice[len(i.Slice)-1] = nil // or the zero value of T
			i.Slice = i.Slice[:len(i.Slice)-1]
		}
	}
}

func (i *IDs) AddIDs(idsToAdd *IDs) {
	i.Slice = append(i.Slice, idsToAdd.Slice...)
}
