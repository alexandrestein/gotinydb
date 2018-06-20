package gotinydb

import (
	"context"
	"encoding/json"
	"time"

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
		Content     string
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
		List           []*ResponseQueryElem
		actualPosition int
		query          *Query
	}

	// ResponseQueryElem defines the response as a pointer
	ResponseQueryElem struct {
		ID             *ID
		ContentAsBytes []byte
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

		// Check that all occurrences have been saved
		if nextAsID.Occurrences(nbFilters) {
			ret.IDs = append(ret.IDs, nextAsID)
		}
		return true
	}, ret
}

// NewID returns a new ID with zero occurrence
func NewID(id string) *ID {
	ret := new(ID)
	ret.Content = id
	ret.occurrences = 0
	return ret
}

func (i *ID) incrementLoop(ctx context.Context) {
	for {
		select {
		case trueIncrement, ok := <-i.ch:
			if !ok {
				return
			}
			if trueIncrement {
				i.occurrences++
			}
		case <-ctx.Done():
			i.occurrences = 0
			i.ch = nil
			return
		}
	}
}

// Increment add +1 to the occurrence counter
func (i *ID) Increment(ctx context.Context) {
	if i.ch == nil {
		i.ch = make(chan bool, 0)
		go i.incrementLoop(ctx)
	}
waitForChanToOpen:
	if i.ch == nil {
		time.Sleep(time.Millisecond)
		goto waitForChanToOpen
	}
	i.ch <- true
}

// Occurrences take care that the channel is empty and all occurrences have been saved
func (i *ID) Occurrences(target int) bool {
	if i.ch == nil {
		return false
	}
	i.ch <- false

	if i.occurrences == target {
		return true
	}
	return false
}

// Less must provide a strict weak ordering.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b
func (i *ID) Less(compareToItem btree.Item) bool {
	compareTo, ok := compareToItem.(*ID)
	if !ok {
		return false
	}

	return (i.Content < compareTo.Content)
}

func (i *ID) treeItem() btree.Item {
	return btree.Item(i)
}

func (i *ID) String() string {
	if i == nil {
		return ""
	}
	return i.Content
}

// NewIDs build a new Ids pointer from a slice of bytes
func NewIDs(idsAsBytes []byte) (*IDs, error) {
	ret := new(IDs)

	if idsAsBytes == nil || len(idsAsBytes) == 0 {
		return ret, nil
	}

	ids := []string{}

	err := json.Unmarshal(idsAsBytes, &ids)
	if err != nil {
		return nil, err
	}

	// Init the channel used to count the number of occurrences of a given ID.
	for _, id := range ids {
		// ret.IDs[i] = NewID(id)
		ret.IDs = append(ret.IDs, NewID(id))
	}

	return ret, nil
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
	// idsAsString := i.Strings()
	// return json.Marshal(&idsAsString)
	return json.Marshal(i.Strings())
}

// MustMarshal convert the given IDs pointer as a slice of bytes or nil if any error
func (i *IDs) MustMarshal() []byte {
	ret, _ := i.Marshal()
	return ret
}

// Strings returns all ID as a slice of string
func (i *IDs) Strings() []string {
	ret := make([]string, len(i.IDs))
	for j, id := range i.IDs {
		ret[j] = id.Content
	}
	return ret
}

// NewResponseQuery build a new ResponseQuery pointer with the given limit
func NewResponseQuery(limit int) *ResponseQuery {
	r := new(ResponseQuery)
	r.List = make([]*ResponseQueryElem, limit)
	return r
}

// Len returns the length of the given response
func (r *ResponseQuery) Len() int {
	return len(r.List)
}

// First is part of the mechanism to run the response in a range statement
func (r *ResponseQuery) First() (i int, id string, objAsByte []byte) {
	if len(r.List) <= 0 {
		return -1, "", nil
	}

	r.actualPosition = 0
	return 0, r.List[0].ID.String(), r.List[0].ContentAsBytes
}

// Next can be used in a range loop statement like `for i, id, objAsByte := c.First(); k != nil; k, v = c.Next() {`
func (r *ResponseQuery) Next() (i int, id string, objAsByte []byte) {
	r.actualPosition++
	return r.next()
}

// Last is part of the mechanism to run the response in a range statement
func (r *ResponseQuery) Last() (i int, id string, objAsByte []byte) {
	lastSlot := len(r.List) - 1
	if lastSlot < 0 {
		return -1, "", nil
	}

	r.actualPosition = lastSlot
	return lastSlot, r.List[lastSlot].ID.String(), r.List[lastSlot].ContentAsBytes
}

// Prev can be used in a range loop statement like `for i, id, objAsByte := c.Last(); k != nil; k, v = c.Prev() {`
func (r *ResponseQuery) Prev() (i int, id string, objAsByte []byte) {
	r.actualPosition--
	return r.next()
}

// Is called by r.Next r.Prev to get their next values
func (r *ResponseQuery) next() (i int, id string, objAsByte []byte) {
	if r.actualPosition >= len(r.List) || r.actualPosition < 0 {
		return -1, "", nil
	}
	return r.actualPosition, r.List[r.actualPosition].ID.String(), r.List[r.actualPosition].ContentAsBytes
}

// All is a function to make easy to do some actions to the set of result.
//
func (r *ResponseQuery) All(fn func(id string, objAsBytes []byte) error) (n int, err error) {
	n = 0
	if r == nil {
		return 0, vars.ErrNotFound
	}

	for _, elem := range r.List {
		if n >= len(r.List) {
			break
		}
		err = fn(elem.ID.String(), elem.ContentAsBytes)
		if err != nil {
			return
		}
		n++
	}
	return
}

// One get one element and put it into the pointer
func (r *ResponseQuery) One(destination interface{}) (id string, err error) {
	if r.actualPosition >= len(r.List) {
		return "", vars.ErrTheResponseIsOver
	}

	id = r.List[r.actualPosition].ID.String()
	err = json.Unmarshal(r.List[r.actualPosition].ContentAsBytes, destination)
	r.actualPosition++

	return id, err
}

// ResetPosition reset the position counter to zero
func (r *ResponseQuery) ResetPosition() {
	r.actualPosition = 0
}

// // All returns all values into a slice of pointer
// func (r *ResponseQuery) All(destination interface{}) error {
// 	for i, objectAsBytes := range r.ObjectsAsBytes {
// 		err := json.Unmarshal(objectAsBytes, destination[i])
// 		if err != nil {
// 			return err
// 		}
// 	}
// }
