package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/google/btree"
)

type (
	// Query defines the object to request index query.
	Query struct {
		filters []*Filter

		orderSelector []string
		order         uint64 // is the selector hash representation
		ascendent     bool   // defines the way of the order

		limit         int
		internalLimit int
		timeout       time.Duration
	}

	// idType is a type to order IDs during query to be compatible with the tree query
	idType struct {
		ID          string
		occurrences int
		ch          chan int
		// values defines the different values and selector that called this ID
		// selectors are defined by a hash 64
		values map[uint64][]byte

		// This is for the ordering
		less         func(btree.Item) bool
		selectorHash uint64
		getRefsFunc  func(id string) *refs
	}

	// idsType defines a list of ID. The struct is needed to build a pointer to be
	// passed to deferent functions
	idsType struct {
		IDs []*idType
	}

	idsTypeMultiSorter struct {
		IDs    []*idType
		invert bool
	}

	// FilterOperator defines the type of filter to perform
	FilterOperator string

	// Response holds the results of a query
	Response struct {
		list           []*ResponseElem
		actualPosition int
		query          *Query
	}

	// ResponseElem defines the response as a pointer
	ResponseElem struct {
		ID             *idType
		ContentAsBytes []byte
	}
)

func (iMs *idsTypeMultiSorter) Sort(limit int) {
	sort.Sort(iMs)

	if iMs.Len() > limit {
		iMs.IDs = iMs.IDs[:limit]
	}
}
func (iMs *idsTypeMultiSorter) Len() int {
	return len(iMs.IDs)
}
func (iMs *idsTypeMultiSorter) Swap(i, j int) {
	iMs.IDs[i], iMs.IDs[j] = iMs.IDs[j], iMs.IDs[i]
}
func (iMs *idsTypeMultiSorter) Less(i, j int) bool {
	if iMs.invert {
		return !iMs.less(i, j)
	}

	return iMs.less(i, j)
}
func (iMs *idsTypeMultiSorter) less(i, j int) bool {
	p, q := iMs.IDs[i], iMs.IDs[j]

	// Compare the order value
	switch comp := bytes.Compare(p.values[p.selectorHash], q.values[q.selectorHash]); comp {
	case -1:
		return true
	case 1:
		return false
		// If equal compare the ID
	case 0:
		switch p.ID < q.ID {
		case true:
			return true
		case false:
			return false
		}
	}
	return false
}

// NewQuery build a new query object.
// It also set the default limit.
func NewQuery() *Query {
	return &Query{
		limit:         DefaultQueryLimit,
		internalLimit: DefaultQueryLimit * 10,
		timeout:       DefaultQueryTimeOut,
	}
}

// SetLimits defines the configurable limit of IDs.
// The first parameters is the limit of the result.
// The second define the internal limit of the query.
// It can be omitted, in this case the internal limit is 10 times the responses limit.
// If you have many many results in the intermediate results this can helps
// you to have more room during query.
// Note that internal limit can't go higher that the database is configured for.
func (q *Query) SetLimits(resultsLimit, internalLimit int) *Query {
	q.limit = resultsLimit
	if internalLimit == 0 {
		internalLimit = resultsLimit * 10
	}
	q.internalLimit = internalLimit
	return q
}

// SetTimeout define the limit in time of the given query.
// It will be canceled after the duration is passed.
func (q *Query) SetTimeout(timeout time.Duration) *Query {
	q.timeout = timeout
	return q
}

// SetOrder defines the order of the response
func (q *Query) SetOrder(ascendent bool, selector ...string) *Query {
	q.orderSelector = selector
	q.order = buildSelectorHash(selector)
	q.ascendent = ascendent
	return q
}

// SetFilter defines the action to perform to get IDs
func (q *Query) SetFilter(f *Filter) *Query {
	if q.filters == nil {
		q.filters = []*Filter{}

	}
	q.filters = append(q.filters, f)
	return q
}

func occurrenceTreeIterator(nbFilters, maxResponse int, orderSelectorHash uint64, getRefsFunc func(id string) *refs) (func(next btree.Item) (over bool), *struct{ IDs []*idType }) {
	ret := &struct{ IDs []*idType }{}
	ret.IDs = []*idType{}
	return func(next btree.Item) bool {
		if len(ret.IDs) >= maxResponse {
			return false
		}

		nextAsID, ok := next.(*idType)
		if !ok {
			return false
		}
		// Check that there is as must occurrences that the number of filters
		if nextAsID.Occurrences(nbFilters) {
			nextAsID.selectorHash = orderSelectorHash
			nextAsID.getRefsFunc = getRefsFunc

			// Get the value we need to index for ordering
			if nextAsID.values[orderSelectorHash] == nil {
				refs := getRefsFunc(nextAsID.ID)
				for _, ref := range refs.Refs {
					if ref.IndexHash == orderSelectorHash {
						nextAsID.values[orderSelectorHash] = ref.IndexedValue
						break
					}
				}
			}

			ret.IDs = append(ret.IDs, nextAsID)
		}
		return true
	}, ret
}

// newID returns a new ID with zero occurrence
func newID(ctx context.Context, id string) *idType {
	ret := new(idType)
	ret.ID = id
	ret.occurrences = 0
	ret.ch = make(chan int, 0)
	ret.values = map[uint64][]byte{}

	go ret.incrementLoop(ctx)

	return ret
}

func (i *idType) incrementLoop(ctx context.Context) {
	for {
		select {
		case indice, ok := <-i.ch:
			if !ok {
				return
			}
			if indice != 0 {
				i.occurrences = i.occurrences + indice
			}
		case <-ctx.Done():
			i.occurrences = 0
			i.ch = nil
			return
		}
	}
}

// Increment add +1 to the occurrence counter
func (i *idType) Increment(indice int) {
	i.ch <- indice
}

// Occurrences take care that the channel is empty and all occurrences have been saved
func (i *idType) Occurrences(target int) bool {
	if i.ch == nil {
		return false
	}
	i.ch <- 0

	if i.occurrences == target {
		return true
	}
	return false
}

// Less implements the btree.Item interface. It can be an indexation
// on the ID or on the value
func (i *idType) Less(compareToItem btree.Item) bool {
	compareTo, ok := compareToItem.(*idType)
	if !ok {
		return false
	}

	return (i.ID < compareTo.ID)
}

func (i *idType) treeItem() btree.Item {
	return btree.Item(i)
}

func (i *idType) String() string {
	if i == nil {
		return ""
	}
	return i.ID
}

// newIDs build a new Ids pointer from a slice of bytes
func newIDs(ctx context.Context, selectorHash uint64, referredValue []byte, idsAsBytes []byte) (*idsType, error) {
	ret := new(idsType)

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
		newID := newID(ctx, id)
		if selectorHash != 0 && referredValue != nil {
			newID.values[selectorHash] = referredValue
		}
		ret.IDs = append(ret.IDs, newID)
	}

	return ret, nil
}

// RmID removes the given ID from the list
func (i *idsType) RmID(idToRm string) {
	for j, id := range i.IDs {
		if id.String() == idToRm {
			copy(i.IDs[j:], i.IDs[j+1:])
			i.IDs[len(i.IDs)-1] = nil // or the zero value of T
			i.IDs = i.IDs[:len(i.IDs)-1]
		}
	}
}

// AddIDs insert multiple ids as IDs pointer into the list
func (i *idsType) AddIDs(idsToAdd *idsType) {
	if len(idsToAdd.IDs) == 0 {
		return
	}
	i.IDs = append(i.IDs, idsToAdd.IDs...)
}

// AddID insert the given ID pointer into the list
func (i *idsType) AddID(idToAdd *idType) {
	if i.IDs == nil {
		i.IDs = []*idType{}
	}
	i.IDs = append(i.IDs, idToAdd)
}

// Marshal convert the given IDs pointer as a slice of bytes or returns an error if any
func (i *idsType) Marshal() ([]byte, error) {
	// idsAsString := i.Strings()
	// return json.Marshal(&idsAsString)
	return json.Marshal(i.Strings())
}

// MustMarshal convert the given IDs pointer as a slice of bytes or nil if any error
func (i *idsType) MustMarshal() []byte {
	ret, _ := i.Marshal()
	return ret
}

// Strings returns all ID as a slice of string
func (i *idsType) Strings() []string {
	ret := make([]string, len(i.IDs))
	for j, id := range i.IDs {
		ret[j] = id.ID
	}
	return ret
}

// newResponse build a new Response pointer with the given limit
func newResponse(limit int) *Response {
	r := new(Response)
	r.list = make([]*ResponseElem, limit)
	return r
}

// Len returns the length of the given response
func (r *Response) Len() int {
	return len(r.list)
}

// First used with Next
func (r *Response) First() (i int, id string, objAsByte []byte) {
	if len(r.list) <= 0 {
		return -1, "", nil
	}

	r.actualPosition = 0
	return 0, r.list[0].ID.String(), r.list[0].ContentAsBytes
}

// Next used with First
func (r *Response) Next() (i int, id string, objAsByte []byte) {
	r.actualPosition++
	return r.next()
}

// Last used with Prev
func (r *Response) Last() (i int, id string, objAsByte []byte) {
	lastSlot := len(r.list) - 1
	if lastSlot < 0 {
		return -1, "", nil
	}

	r.actualPosition = lastSlot
	return lastSlot, r.list[lastSlot].ID.String(), r.list[lastSlot].ContentAsBytes
}

// Prev used with Last
func (r *Response) Prev() (i int, id string, objAsByte []byte) {
	r.actualPosition--
	return r.next()
}

// Is called by r.Next r.Prev to get their next values
func (r *Response) next() (i int, id string, objAsByte []byte) {
	if r.actualPosition >= len(r.list) || r.actualPosition < 0 {
		r.actualPosition = 0
		return -1, "", nil
	}
	return r.actualPosition, r.list[r.actualPosition].ID.String(), r.list[r.actualPosition].ContentAsBytes
}

// All takes a function as argument and permit to unmarshal or to manage recoredes inside the function
func (r *Response) All(fn func(id string, objAsBytes []byte) error) (n int, err error) {
	n = 0
	if r == nil {
		return 0, ErrNotFound
	}

	for _, elem := range r.list {
		if n >= len(r.list) {
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

// One retrieve one element at the time and put it into the destination pointer.
// Use it to get the objects one after the other.
func (r *Response) One(destination interface{}) (id string, err error) {
	if r.actualPosition >= len(r.list) {
		r.actualPosition = 0
		return "", ErrTheResponseIsOver
	}

	id = r.list[r.actualPosition].ID.String()
	err = json.Unmarshal(r.list[r.actualPosition].ContentAsBytes, destination)
	r.actualPosition++

	return id, err
}

// GetID return the ID as string of the given element
func (r *ResponseElem) GetID() string {
	return r.ID.ID
}
