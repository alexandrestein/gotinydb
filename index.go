package gotinydb

import (
	"context"
	"encoding/json"
	"log"

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

		// getIDsFunc      func(ctx context.Context, indexedValue []byte) (*IDs, error)
		// getRangeIDsFunc func(ctx context.Context, indexedValue []byte, keepEqual, increasing bool) (*IDs, error)
		getTx func(update bool) (*bolt.Tx, error)
	}

	// Refs defines an struct to manage the references of a given object
	// in all the indexes it belongs to
	Refs struct {
		ObjectID     string
		ObjectHashID string

		Refs []*Ref
	}

	// Ref defines the relations between a object with some index with indexed value
	Ref struct {
		IndexName    string
		IndexHash    uint64
		IndexedValue []byte
	}
)

// NewIndex build a new Index pointer
func NewIndex(name string, selector []string, t vars.IndexType) *Index {
	ret := new(Index)
	ret.Name = name
	ret.Selector = selector
	ret.SelectorHash = vars.BuildSelectorHash(selector)
	ret.Type = t

	return ret
}

// Apply take the full object to add in the collection and check if is must be
// indexed or not. If the object needs to be indexed the value to index is returned as a byte slice.
func (i *Index) Apply(object interface{}) (contentToIndex []byte, ok bool) {
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

// DoesFilterApplyToIndex only check if the filter belongs to the index
func (i *Index) DoesFilterApplyToIndex(filter *Filter) (ok bool) {
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
	case vars.BytesIndex:
		contentToIndex, ok = value.([]byte)
		if ok {
			if contentToIndex == nil || len(contentToIndex) <= 0 {
				return nil, false
			}
		}
		return
	default:
		return nil, false
	}
	var err error
	if contentToIndex, err = conversionFunc(value); err != nil {
		return nil, false
	}
	return contentToIndex, true
}

// Query do the given filter and ad it to the tree
func (i *Index) Query(ctx context.Context, filter *Filter, finishedChan chan *IDs) {
	done := false
	defer func() {
		// Make sure to reply as done
		if !done {
			finishedChan <- nil
			return
		}
	}()

	ids, _ := NewIDs(ctx, filter.selectorHash, nil, nil)

	switch filter.GetType() {
	// If equal just this leave will be send
	case Equal:
		for _, value := range filter.values {
			tmpIDs, getErr := i.getIDsForOneValue(ctx, value.Bytes())
			if getErr != nil {
				log.Printf("Index.runQuery Equal: %s\n", getErr.Error())
				return
			}

			for _, tmpID := range tmpIDs.IDs {
				tmpID.values[i.SelectorHash] = value.Bytes()

			}

			ids.AddIDs(tmpIDs)
		}

	case Greater, Less:
		greater := true
		if filter.GetType() == Less {
			greater = false
		}

		tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), nil, filter.equal, greater)
		if getIdsErr != nil {
			log.Printf("Index.runQuery Greater, Less: %s\n", getIdsErr.Error())
			return
		}

		ids.AddIDs(tmpIDs)

	case Between:
		// Needs two values to make between
		if len(filter.values) < 2 {
			return
		}
		tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), filter.values[1].Bytes(), filter.equal, true)
		if getIdsErr != nil {
			log.Printf("Index.runQuery Between: %s\n", getIdsErr.Error())
			return
		}

		ids.AddIDs(tmpIDs)
	}

	finishedChan <- ids
	done = true

	return
}

// NewRefs builds a new empty Refs pointer
func NewRefs() *Refs {
	refs := new(Refs)
	refs.Refs = []*Ref{}
	return refs
}

// NewRefsFromDB builds a Refs pointer based on the saved value in database
func NewRefsFromDB(input []byte) *Refs {
	refs := new(Refs)
	json.Unmarshal(input, refs)
	return refs
}

// IDasBytes returns the ID of the coresponding object as a slice of bytes
func (r *Refs) IDasBytes() []byte {
	return []byte(r.ObjectHashID)
}

// SetIndexedValue add to the list of references this one.
// The indexName define the index it belongs to and indexedVal defines what value
// is indexed.
func (r *Refs) SetIndexedValue(indexName string, indexHash uint64, indexedVal []byte) {
	for _, ref := range r.Refs {
		if ref.IndexName == indexName {
			ref.IndexedValue = indexedVal
			return
		}
	}

	ref := new(Ref)
	ref.IndexName = indexName
	ref.IndexHash = indexHash
	ref.IndexedValue = indexedVal
	r.Refs = append(r.Refs, ref)
}

// AsBytes marshals the given Refs pointer into a slice of bytes fo saving
func (r *Refs) AsBytes() []byte {
	ret, _ := json.Marshal(r)
	return ret
}
