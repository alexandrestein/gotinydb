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
		IDs []*ID
	}

	// ActionType defines the type of action to perform.
	ActionType string

	ResponseQuery struct {
		IDs            []*ID
		ObjectsAsBytes [][]byte
	}
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
	q.getActions = q.addAction(a, q.getActions)
	return q
}

// Clean defines the actions to perform to clean IDs which have already retrieved
// by the Get actions.
func (q *Query) Clean(a *Action) *Query {
	q.cleanActions = q.addAction(a, q.cleanActions)
	return q
}
func (q *Query) addAction(a *Action, list []*Action) []*Action {
	if list == nil {
		list = []*Action{}
	}
	if a.limit <= 0 {
		a.limit = q.limit
	}

	list = append(list, a)
	return list
}

func iterator(maxResponse int) (func(next btree.Item) (over bool), *IDs) {
	ret := new(IDs)

	return func(next btree.Item) bool {
		if len(ret.IDs) >= maxResponse {
			return false
		}

		nextAsID, ok := next.(*ID)
		if !ok {
			return false
		}

		ret.IDs = append(ret.IDs, nextAsID)
		return true
	}, ret
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

// NewIDs build a new Ids pointer from a slice of bytes
func NewIDs(idsAsBytes []byte) (*IDs, error) {
	ids := new(IDs)

	if idsAsBytes == nil || len(idsAsBytes) == 0 {
		ids.IDs = []*ID{}
		return ids, nil
	}

	err := json.Unmarshal(idsAsBytes, ids)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

func (i *IDs) SetID(idToSet string) {
	id := ID(idToSet)
	i.IDs = append(i.IDs, &id)
}

func (i *IDs) RmID(idToRm string) {
	for j, id := range i.IDs {
		if id.String() == idToRm {
			copy(i.IDs[j:], i.IDs[j+1:])
			i.IDs[len(i.IDs)-1] = nil // or the zero value of T
			i.IDs = i.IDs[:len(i.IDs)-1]
		}
	}
}

func (i *IDs) AddIDs(idsToAdd *IDs) {
	i.IDs = append(i.IDs, idsToAdd.IDs...)
}

func (i *IDs) AddID(idToAdd *ID) {
	if i.IDs == nil {
		i.IDs = []*ID{}
	}
	i.IDs = append(i.IDs, idToAdd)
}

func (i *IDs) Marshal() ([]byte, error) {
	return json.Marshal(i)
}

func NewResponseQuery(limit int) *ResponseQuery {
	r := new(ResponseQuery)
	r.IDs = make([]*ID, limit)
	r.ObjectsAsBytes = make([][]byte, limit)
	return r
}

func (r *ResponseQuery) Len() int {
	return len(r.IDs)
}

func (r *ResponseQuery) Range(fn func(id string, objAsBytes []byte)) (n int) {
	n = 0
	for i, id := range r.IDs {
		objAsBytes := r.ObjectsAsBytes[i]
		if objAsBytes == nil {
			break
		}
		fn(id.String(), objAsBytes)
		n++
	}
	return
}
