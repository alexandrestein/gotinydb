package gotinydb

import (
	"context"

	"github.com/alexandrestein/gods/trees/btree"
	"github.com/alexandrestein/gotinydb/vars"
	"github.com/fatih/structs"
)

type (
	// Index defines the struct to manage indexation
	Index struct {
		Name     string
		Selector []string
		Type     vars.IndexType
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

func (i *Index) RunQuery(ctx context.Context, actions []*Action, retChan chan []string) {
	responseChan := make(chan []string, 16)
	defer close(retChan)
	defer close(responseChan)

	if len(actions) == 0 {
		return
	}

	nbToWait := 0
	for _, action := range actions {
		if !i.doesApply(action.Selector) {
			continue
		}

		go getIDs(ctx, i, action, responseChan)
		nbToWait++
	}

	ret := []string{}

	for {
		select {structIndex
		case ids := <-responseChan:
			ret = append(ret, ids...)
			retChan <- ret
		case <-ctx.Done():
			return
		}
		nbToWait--
		if nbToWait <= 0 {
			return
		}
	}
}

func getIDs(ctx context.Context, i *Index, action *Action, responseChan chan []string) {
	ids := i.runQuery(action)
	responseChan <- ids
}

func (i *Index) runQuery(action *Action) (ids []string) {
	// If equal just this leave will be send
	if action.GetType() == Equal {
		tmpIDs, found := i.Get(action.CompareToValue)
		if found {
			ids = tmpIDs
		}
		return
	}

	var iterator btree.Iterator
	var iteratorInit bool
	var nextFunc (func() bool)
	var keyFound bool

	if action.GetType() == Greater {
		_, keyAfter, found := i.tree.GetClosestKeys(action.CompareToValue)
		keyFound = found
		if keyAfter != nil {
			iterator, _ = i.tree.IteratorAt(keyAfter)
			iteratorInit = true
		}
		nextFunc = iterator.Next
	} else if action.GetType() == Less {
		keyBefore, _, found := i.tree.GetClosestKeys(action.CompareToValue)
		keyFound = found
		if keyBefore != nil {
			iterator, _ = i.tree.IteratorAt(keyBefore)
			iteratorInit = true
		}
		nextFunc = iterator.Prev
	}

	// Check if the caller want more or less with equal option
	if keyFound {
		if !action.KeepEqual {
			ids = append(ids, iterator.Value().([]string)...)
		}
	} else {
		if iteratorInit {
			ids = append(ids, iterator.Value().([]string)...)
		}
	}

	if !iteratorInit {
		return
	}

	for nextFunc() {
		ids = append(ids, iterator.Value().([]string)...)
	}

	return
}
