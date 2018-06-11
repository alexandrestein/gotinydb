package gotinydb

import (
	"context"
	"encoding/json"
	"log"

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

	Refs struct {
		ObjectID     string
		ObjectHashID string

		Refs []*Ref
	}

	Ref struct {
		IndexName    string
		IndexedValue []byte
	}
)

// Apply take the full object to add in the collection and check if is must be
// indexed or not. If the object needs to be indexed the value to index is returned as a byte slice.
func (i *Index) Apply(object interface{}) (contentToIndex []byte, ok bool) {
	objectAsMap := structs.Map(object)
	// var intermediatObject interface{}
	for _, fieldName := range i.Selector {
		object = objectAsMap[fieldName]
		if object == nil {
			return nil, false
		}
	}
	return i.testType(object)
}

func (i *Index) testType(value interface{}) (contentToIndex []byte, ok bool) {
	var convFunc func(interface{}) ([]byte, error)
	switch i.Type {
	case vars.StringIndex:
		convFunc = vars.StringToBytes
	case vars.IntIndex:
		convFunc = vars.IntToBytes
	case vars.FloatIndex:
		convFunc = vars.FloatToBytes
	case vars.TimeIndex:
		convFunc = vars.TimeToBytes
	case vars.BytesIndex:
		contentToIndex, ok = value.([]byte)
		return
	default:
		return nil, false
	}
	var err error
	if contentToIndex, err = convFunc(value); err != nil {
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
		}
	}()

	var ids *IDs

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
		ids = tmpIDs
		goto addToTree
	} else if action.GetType() == Less {
		tmpIDs, getIdsErr := i.getRangeIDsFunc(action.ValueToCompareAsBytes(), action.equal, false, action.limit)
		if getIdsErr != nil {
			log.Printf("Index.runQuery Less: %s\n", getIdsErr.Error())
			return
		}
		ids = tmpIDs
		goto addToTree
	}

addToTree:

	finishedChan <- ids
	done = true

	return
}

// doesApply check the action selector to define if yes or not the index
// needs to be called
func (i *Index) doesApply(action *Action) bool {
	for j, fieldName := range i.Selector {
		if action.selector[j] != fieldName {
			return false
		}
	}
	return true
}

func NewRefs() *Refs {
	refs := new(Refs)
	refs.Refs = []*Ref{}
	return refs
}

func NewRefsFromDB(input []byte) *Refs {
	refs := new(Refs)
	json.Unmarshal(input, refs)
	return refs
}

func (r *Refs) IDasBytes() []byte {
	return []byte(r.ObjectHashID)
}

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

func (r *Refs) DelIndexedValue(indexName string) {
	for i, ref := range r.Refs {
		if ref.IndexName == indexName {
			copy(r.Refs[i:], r.Refs[i+1:])
			r.Refs[len(r.Refs)-1] = nil
			r.Refs = r.Refs[:len(r.Refs)-1]
			return
		}
	}
}

func (r *Refs) AsBytes() []byte {
	ret, _ := json.Marshal(r)
	return ret
}
