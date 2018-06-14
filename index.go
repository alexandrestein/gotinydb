package gotinydb

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/fatih/structs"
)

type (
	// Index defines the struct to manage indexation
	Index struct {
		Name     string
		Selector []string
		Type     vars.IndexType

		getIDsFunc      func(indexedValue []byte) (*IDs, error)
		getRangeIDsFunc func(indexedValue []byte, keepEqual, increasing bool, nb int) (*IDs, error)
		setIDFunc       func(indexedValue []byte, id string) error
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
		IndexedValue []byte
	}
)

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

// QueryApplyToIndex only check if the action belongs to the index
func (i *Index) QueryApplyToIndex(action *Action) (ok bool) {
	for j := range i.Selector {
		if action.selector[j] != i.Selector[j] {
			return false
		}
	}

	switch action.compareToValue.(type) {
	case string:
		if i.Type == vars.StringIndex {
			return true
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		if i.Type == vars.IntIndex {
			return true
		}
	case time.Time:
		if i.Type == vars.TimeIndex {
			return true
		}
	case []byte:
		if i.Type == vars.BytesIndex {
			return true
		}
	default:
		return false
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

// Query do the given actions and ad it to the tree
func (i *Index) Query(ctx context.Context, action *Action, finishedChan chan *IDs) {
	done := false
	// Make sure to reply as over
	defer func() {
		if !done {
			finishedChan <- nil
			return
		}
	}()

	ids, _ := NewIDs(nil)

	// If equal just this leave will be send
	if action.GetType() == Equal {
		tmpIDs, getErr := i.getIDsFunc(action.ValueToCompareAsBytes())
		if getErr != nil {
			log.Printf("Index.runQuery Equal: %s\n", getErr.Error())
			return
		}

		ids.AddIDs(tmpIDs)
		goto addToTree
	}

	if action.GetType() == Greater {
		tmpIDs, getIdsErr := i.getRangeIDsFunc(action.ValueToCompareAsBytes(), action.equal, true, action.limit)
		if getIdsErr != nil {
			log.Printf("Index.runQuery Greater: %s\n", getIdsErr.Error())
			return
		}

		ids.AddIDs(tmpIDs)
		goto addToTree
	} else if action.GetType() == Less {
		tmpIDs, getIdsErr := i.getRangeIDsFunc(action.ValueToCompareAsBytes(), action.equal, false, action.limit)
		if getIdsErr != nil {
			log.Printf("Index.runQuery Less: %s\n", getIdsErr.Error())
			return
		}

		ids.AddIDs(tmpIDs)
		goto addToTree
	}

addToTree:
	if len(ids.IDs) > action.limit {
		ids.IDs = ids.IDs[:action.limit]
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
func (r *Refs) SetIndexedValue(indexName string, indexedVal []byte) {
	for _, ref := range r.Refs {
		if ref.IndexName == indexName {
			ref.IndexedValue = indexedVal
			return
		}
	}

	ref := new(Ref)
	ref.IndexName = indexName
	ref.IndexedValue = indexedVal
	r.Refs = append(r.Refs, ref)
}

// AsBytes marshals the given Refs pointer into a slice of bytes fo saving
func (r *Refs) AsBytes() []byte {
	ret, _ := json.Marshal(r)
	return ret
}
