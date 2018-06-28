package gotinydb

import (
	"context"
	"encoding/json"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/boltdb/bolt"
	"github.com/fatih/structs"
)

type (
	// Index defines the struct to manage indexation
	Index struct {
		Name         string
		Selector     []string
		SelectorHash uint64
		Type         vars.IndexType

		conf *Conf

		getTx func(update bool) (*bolt.Tx, error)
	}

	// refs defines an struct to manage the references of a given object
	// in all the indexes it belongs to
	refs struct {
		ObjectID     string
		ObjectHashID string

		Refs []*ref
	}

	// Ref defines the relations between a object with some index with indexed value
	ref struct {
		IndexName    string
		IndexHash    uint64
		IndexedValue []byte
	}
)

// NewIndex build a new Index pointer
func NewIndex(name string, t vars.IndexType, selector ...string) *Index {
	ret := new(Index)
	ret.Name = name
	ret.Selector = selector
	ret.SelectorHash = vars.BuildSelectorHash(selector)
	ret.Type = t

	return ret
}

// apply take the full object to add in the collection and check if is must be
// indexed or not. If the object needs to be indexed the value to index is returned as a byte slice.
func (i *Index) apply(object interface{}) (contentToIndex []byte, ok bool) {
	structObj := structs.New(object)
	var field *structs.Field
	for i, fieldName := range i.Selector {
		if i == 0 {
			field, ok = structObj.FieldOk(fieldName)
		} else {
			field, ok = field.FieldOk(fieldName)
		}
		if !ok {
			return nil, false
		}
	}
	return i.testType(field.Value())
}

// doesFilterApplyToIndex only check if the filter belongs to the index
func (i *Index) doesFilterApplyToIndex(filter *Filter) (ok bool) {
	// Check the selector
	if filter.selectorHash != i.SelectorHash {
		return false
	}

	// If at least one of the value has the right type the index need to be queried
	for _, value := range filter.values {
		if value.Type == i.Type {
			return true
		}
	}

	return false
}

func (i *Index) testType(value interface{}) (contentToIndex []byte, ok bool) {
	var conversionFunc func(interface{}) ([]byte, error)
	switch i.Type {
	case vars.StringIndex:
		conversionFunc = vars.StringToBytes
	case vars.IntIndex:
		conversionFunc = vars.IntToBytes
	case vars.TimeIndex:
		conversionFunc = vars.TimeToBytes
	default:
		return nil, false
	}
	var err error
	if contentToIndex, err = conversionFunc(value); err != nil {
		return nil, false
	}
	return contentToIndex, true
}

// query do the given filter and ad it to the tree
func (i *Index) query(ctx context.Context, filter *Filter, finishedChan chan *idsType) {
	done := false
	defer func() {
		// Make sure to reply as done
		if !done && ctx.Err() == nil {
			finishedChan <- nil
			return
		}
	}()

	ids, _ := newIDs(ctx, filter.selectorHash, nil, nil)

	switch filter.GetType() {
	// If equal just this leave will be send
	case Equal:
		i.queryEqual(ctx, ids, filter)
		// for _, value := range filter.values {
		// 	tmpIDs, getErr := i.getIDsForOneValue(ctx, value.Bytes())
		// 	if getErr != nil {
		// 		log.Printf("Index.runQuery Equal: %s\n", getErr.Error())
		// 		return
		// 	}

		// 	for _, tmpID := range tmpIDs.IDs {
		// 		tmpID.values[i.SelectorHash] = value.Bytes()

		// 	}

		// 	ids.AddIDs(tmpIDs)
		// }

	case Greater, Less:
		i.queryGreaterLess(ctx, ids, filter)
		// greater := true
		// if filter.GetType() == Less {
		// 	greater = false
		// }

		// tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), nil, filter.equal, greater)
		// if getIdsErr != nil {
		// 	log.Printf("Index.runQuery Greater, Less: %s\n", getIdsErr.Error())
		// 	return
		// }

		// ids.AddIDs(tmpIDs)

	case Between:
		i.queryBetween(ctx, ids, filter)
		// // Needs two values to make between
		// if len(filter.values) < 2 {
		// 	return
		// }
		// tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), filter.values[1].Bytes(), filter.equal, true)
		// if getIdsErr != nil {
		// 	log.Printf("Index.runQuery Between: %s\n", getIdsErr.Error())
		// 	return
		// }

		// ids.AddIDs(tmpIDs)
	}

	// Force to check first if a cancel signal has been send
	// If not already canceled it wait for done or cancel
	select {
	case <-ctx.Done():
		return
	default:
		select {
		case finishedChan <- ids:
		case <-ctx.Done():
			return
		}
	}

	done = true

	return
}

// newRefs builds a new empty Refs pointer
func newRefs() *refs {
	refs := new(refs)
	refs.Refs = []*ref{}
	return refs
}

// newRefsFromDB builds a Refs pointer based on the saved value in database
func newRefsFromDB(input []byte) *refs {
	refs := new(refs)
	json.Unmarshal(input, refs)
	return refs
}

// IDasBytes returns the ID of the coresponding object as a slice of bytes
func (r *refs) IDasBytes() []byte {
	return []byte(r.ObjectHashID)
}

// setIndexedValue add to the list of references this one.
// The indexName define the index it belongs to and indexedVal defines what value
// is indexed.
func (r *refs) setIndexedValue(indexName string, indexHash uint64, indexedVal []byte) {
	for _, ref := range r.Refs {
		if ref.IndexName == indexName {
			ref.IndexedValue = indexedVal
			return
		}
	}

	ref := new(ref)
	ref.IndexName = indexName
	ref.IndexHash = indexHash
	ref.IndexedValue = indexedVal
	r.Refs = append(r.Refs, ref)
}

// asBytes marshals the given Refs pointer into a slice of bytes fo saving
func (r *refs) asBytes() []byte {
	ret, _ := json.Marshal(r)
	return ret
}
