package gotinydb

import (
	"encoding/json"

	"github.com/alexandrestein/gotinydb/vars"
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

		// orderBy       []string
		// revertedOrder bool

		limit int
	}

	// ID is a type to order IDs during query to be compatible with the tree query
	ID string

	// IDs defines a list of ID. The struct is needed to build a pointer to be
	// passed to deferent functions
	IDs struct {
		IDs []*ID
	}

	// ActionType defines the type of action to perform
	ActionType string

	// ResponseQuery holds the results of a query
	ResponseQuery struct {
		IDs            []*ID
		ObjectsAsBytes [][]byte

		actualPosition int
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

// DistinctWanted clean the duplicated IDs
func (q *Query) DistinctWanted() *Query {
	// q.distinct = true
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
	if i == nil {
		return ""
	}
	return string(*i)
}

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

// RmID removes the given ID from the list
func (i *IDs) RmID(idToRm string) {
	for j, id := range i.IDs {
		if id.String() == idToRm {
			copy(i.IDs[j:], i.IDs[j+1:])
			i.IDs[len(i.IDs)-1] = nil // or the zero value of T
			i.IDs = i.IDs[:len(i.IDs)-1]
		}
	}
}

// AddIDs insert mulitple ids as IDs pointer into the list
func (i *IDs) AddIDs(idsToAdd *IDs) {
	i.IDs = append(i.IDs, idsToAdd.IDs...)
}

// AddID insert the given ID pointer into the list
func (i *IDs) AddID(idToAdd *ID) {
	if i.IDs == nil {
		i.IDs = []*ID{}
	}
	i.IDs = append(i.IDs, idToAdd)
}

// Marshal convert the given IDs pointer as a slice of bytes or returns an error if any
func (i *IDs) Marshal() ([]byte, error) {
	return json.Marshal(i)
}

// MustMarshal convert the given IDs pointer as a slice of bytes or nil if any error
func (i *IDs) MustMarshal() []byte {
	ret, _ := json.Marshal(i)
	return ret
}

// NewResponseQuery build a new ResponseQuery pointer with the given limit
func NewResponseQuery(limit int) *ResponseQuery {
	r := new(ResponseQuery)
	r.IDs = make([]*ID, limit)
	r.ObjectsAsBytes = make([][]byte, limit)
	return r
}

// Len returns the length of the given response
func (r *ResponseQuery) Len() int {
	return len(r.IDs)
}

// First is part of the mechanism to run the response in a range statement
func (r *ResponseQuery) First() (i int, id string, objAsByte []byte) {
	if 0 >= len(r.IDs) || 0 >= len(r.ObjectsAsBytes) {
		return 0, "", nil
	}
	r.actualPosition = 0
	return 0, r.IDs[0].String(), r.ObjectsAsBytes[0]
}

// Next can be used in a range loop statement like `for i, id, objAsByte := c.First(); k != nil; k, v = c.Next() {`
func (r *ResponseQuery) Next() (i int, id string, objAsByte []byte) {
	i = r.actualPosition
	r.actualPosition++
	if i >= len(r.IDs) || i >= len(r.ObjectsAsBytes) {
		return 0, "", nil
	}
	return i, r.IDs[i].String(), r.ObjectsAsBytes[i]
}

// Range is a function to make easy to do some actions to the set of result
func (r *ResponseQuery) Range(fn func(id string, objAsBytes []byte) error) (n int, err error) {
	n = 0
	if r == nil {
		return 0, vars.ErrNotFound
	}
	for i, id := range r.IDs {
		objAsBytes := r.ObjectsAsBytes[i]
		if objAsBytes == nil {
			break
		}
		err = fn(id.String(), objAsBytes)
		if err != nil {
			return
		}
		n++
	}
	return
}
