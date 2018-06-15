package gotinydb

import (
	"encoding/json"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/google/btree"
)

// Those constants defines the different types of filter to perform at query
const (
	Equal    FilterOperator = "eq"
	NotEqual FilterOperator = "ne"
	Greater  FilterOperator = "gr"
	Less     FilterOperator = "le"
)

type (
	// Query defines the object to request index query.
	Query struct {
		filters []*Filter

		limit int
	}

	// ID is a type to order IDs during query to be compatible with the tree query
	ID struct {
		content     string
		occurrences int
		ch          chan bool
	}

	// IDs defines a list of ID. The struct is needed to build a pointer to be
	// passed to deferent functions
	IDs struct {
		IDs []*ID
	}

	// FilterOperator defines the type of filter to perform
	FilterOperator string

	// ResponseQuery holds the results of a query
	ResponseQuery struct {
		IDs            []*ID
		ObjectsAsBytes [][]byte

		actualPosition int
		query          *Query
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
	return q
}

// Get defines the action to perform to get IDs
func (q *Query) Get(f *Filter) *Query {
	if q.filters == nil {
		q.filters = []*Filter{}

	}
	q.filters = append(q.filters, f)
	return q
}

func iterator(nbFilters, maxResponse int) (func(next btree.Item) (over bool), *IDs) {
	ret := new(IDs)

	return func(next btree.Item) bool {
		if len(ret.IDs) >= maxResponse {
			return false
		}

		nextAsID, ok := next.(*ID)
		if !ok {
			return false
		}

		if nextAsID.occurrences == nbFilters {
			ret.IDs = append(ret.IDs, nextAsID)
		}
		return true
	}, ret
}

// NewID returns a new ID with zero occurrence
func NewID(id string) *ID {
	ret := new(ID)
	ret.content = id
	ret.occurrences = 0
	ret.ch = make(chan bool, 5)
	go ret.incrementLoop()
	return ret
}

func (i *ID) incrementLoop() {
	for {
		_, ok := <-i.ch
		if !ok {
			return
		}
		i.occurrences++
	}
}

// Increment add +1 to the occurrence counter
func (i *ID) Increment() {
	i.ch <- true
}

// Less must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b
func (i *ID) Less(compareToItem btree.Item) bool {
	compareTo, ok := compareToItem.(*ID)
	if !ok {
		return false
	}

	return (i.content < compareTo.content)
}

func (i *ID) treeItem() btree.Item {
	return btree.Item(i)
}

func (i *ID) String() string {
	if i == nil {
		return ""
	}
	return i.content
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

// AddIDs insert multiple ids as IDs pointer into the list
func (i *IDs) AddIDs(idsToAdd *IDs) {
	if len(idsToAdd.IDs) == 0 {
		return
	}
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

// Strings returns all ID as a slice of string
func (i *IDs) Strings() []string {
	ret := make([]string, len(i.IDs))
	for j, id := range i.IDs {
		ret[j] = id.content
	}
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
