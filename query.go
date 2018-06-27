package gotinydb

import (
	"bytes"
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
	Between  FilterOperator = "bw"
)

type (
	// Query defines the object to request index query.
	Query struct {
		filters []*Filter

		order     uint64 // is the selector hash representation
		ascendent bool   // defines the way of the order

		limit         int
		internalLimit int
		timeout       time.Duration
	}

	// ID is a type to order IDs during query to be compatible with the tree query
	ID struct {
		ID          string
		occurrences int
		ch          chan bool
		// values defines the different values and selector that called this ID
		// selectors are defined by a hash 64
		values map[uint64][]byte

		// This is for the ordering
		less         func(btree.Item) bool
		selectorHash uint64
		getRefsFunc  func(id string) *Refs
	}

	// IDs defines a list of ID. The struct is needed to build a pointer to be
	// passed to deferent functions
	IDs struct {
		IDs []*ID
	}

	idsForOrderTree struct {
		value     []byte
		ids       []*ID
		indexHash uint64
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

func newIDsForOrderTree(orderValue []byte, indexHash uint64) *idsForOrderTree {
	i := new(idsForOrderTree)
	i.value = orderValue
	i.ids = []*ID{}
	i.indexHash = indexHash
	return i
}

func (i *idsForOrderTree) addID(id *ID) {
	i.ids = append(i.ids, id)
}

func (i *idsForOrderTree) Less(compareToItem btree.Item) bool {
	compareTo, ok := compareToItem.(*idsForOrderTree)
	if !ok {
		return false
	}

	if len(i.value) == 0 && len(i.ids) > 0 {
		refs := i.ids[0].getRefsFunc(i.ids[0].ID)
		for _, ref := range refs.Refs {
			if i.indexHash == ref.IndexHash {
				i.value = ref.IndexedValue
			}
		}
	}

	if bytes.Compare(i.value, compareTo.value) < 0 {
		return true
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
// The first paramiters is the limit of the result.
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
func (q *Query) SetOrder(selector []string, ascendent bool) *Query {
	q.order = vars.BuildSelectorHash(selector)
	q.ascendent = ascendent
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

func occurrenceTreeIterator(nbFilters, maxResponse int, orderSelectorHash uint64, getRefsFunc func(id string) *Refs) (func(next btree.Item) (over bool), *btree.BTree) {
	ret := btree.New(5)

	return func(next btree.Item) bool {
		if ret.Len() >= maxResponse {
			return false
		}

		nextAsID, ok := next.(*ID)
		if !ok {
			return false
		}
		// Check that there is as must occurrences that the number of filters
		if nextAsID.Occurrences(nbFilters) {
			nextAsID.selectorHash = orderSelectorHash
			nextAsID.getRefsFunc = getRefsFunc

			if nextAsID.values[orderSelectorHash] == nil {
				refs := getRefsFunc(nextAsID.ID)
				for _, ref := range refs.Refs {
					if ref.IndexHash == orderSelectorHash {
						nextAsID.values[orderSelectorHash] = ref.IndexedValue
						break
					}
				}
			}

			ids := newIDsForOrderTree(nextAsID.values[orderSelectorHash], orderSelectorHash)
			idsFromTree := ret.Get(ids)
			if idsFromTree == nil {
				ids.addID(nextAsID)
				ret.ReplaceOrInsert(ids)
				return true
			}
			idsFromTree.(*idsForOrderTree).addID(nextAsID)
		}
		return true
	}, ret
}

func orderTreeIterator(maxResponse int) (func(next btree.Item) (over bool), *IDs) {
	ret := new(IDs)

	return func(next btree.Item) bool {
		if len(ret.IDs) >= maxResponse {
			return false
		}

		nextAsIDs, ok := next.(*idsForOrderTree)
		if !ok {
			return false
		}

		for _, id := range nextAsIDs.ids {
			ret.IDs = append(ret.IDs, id)
			if len(ret.IDs) >= maxResponse {
				return false
			}
		}

		return true
	}, ret
}

// NewID returns a new ID with zero occurrence
func NewID(ctx context.Context, id string) *ID {
	ret := new(ID)
	ret.ID = id
	ret.occurrences = 0
	ret.ch = make(chan bool, 0)
	ret.values = map[uint64][]byte{}

	go ret.incrementLoop(ctx)

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
func (i *ID) Increment() {
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

// Less implements the btree.Item interface. It can be an indexation
// on the ID or on the value
func (i *ID) Less(compareToItem btree.Item) bool {
	compareTo, ok := compareToItem.(*ID)
	if !ok {
		return false
	}

	return (i.ID < compareTo.ID)
}

func (i *ID) treeItem() btree.Item {
	return btree.Item(i)
}

func (i *ID) String() string {
	if i == nil {
		return ""
	}
	return i.ID
}

// NewIDs build a new Ids pointer from a slice of bytes
func NewIDs(ctx context.Context, selectorHash uint64, referredValue []byte, idsAsBytes []byte) (*IDs, error) {
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
		newID := NewID(ctx, id)
		if selectorHash != 0 && referredValue != nil {
			newID.values[selectorHash] = referredValue
		}
		ret.IDs = append(ret.IDs, newID)
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
		ret[j] = id.ID
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
		r.actualPosition = 0
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
		r.actualPosition = 0
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
