package gotinydb

import (
	"github.com/alexandrestein/gotinydb/vars"
	"github.com/fatih/structs"
)

type (
	// Index defines the struct to manage indexation
	Index struct {
		Name     string
		Selector []string
		Type     vars.IndexType

		getIDFunc  func(indexedValue []byte) ([]string, error)
		getIDsFunc func(indexedValue []byte, keepEqual, increasing bool, nb int) ([]string, error)
		addIDFunc  func(indexedValue []byte, id string) error
		rmIDFunc   func(indexedValue []byte, id string) error
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

// // RunQuery runs the given query on the given index
// func (i *Index) RunQuery(ctx context.Context, actions []*Action, retChan chan []string) {
// 	responseChan := make(chan []string, 16)
// 	defer close(retChan)
// 	defer close(responseChan)

// 	if len(actions) == 0 {
// 		return
// 	}

// 	nbToWait := 0
// 	for _, action := range actions {
// 		if !i.doesApply(action) {
// 			continue
// 		}

// 		go getIDs(ctx, i, action, responseChan)
// 		nbToWait++
// 	}

// 	ret := []string{}

// 	for {
// 		select {
// 		case ids := <-responseChan:
// 			ret = append(ret, ids...)
// 			retChan <- ret
// 		case <-ctx.Done():
// 			return
// 		}
// 		nbToWait--
// 		if nbToWait <= 0 {
// 			return
// 		}
// 	}
// }

// func getIDs(ctx context.Context, i *Index, action *Action, responseChan chan []string) {
// 	ids := i.runQuery(action)
// 	responseChan <- ids
// }

// func (i *Index) runQuery(action *Action) (ids []string) {
// 	// If equal just this leave will be send
// 	if action.GetType() == Equal {
// 		tmpIDs, getErr := i.getIDFunc(action.ValueToCompareAsBytes())
// 		if getErr != nil {
// 			log.Printf("Index.runQuery Equal: %s\n", getErr.Error())
// 			log.Println(string(action.ValueToCompareAsBytes()))
// 			return []string{}
// 		}
// 		return tmpIDs
// 	}

// 	if action.GetType() == Greater {
// 		tmpIDs, getIdsErr := i.getIDsFunc(action.ValueToCompareAsBytes(), action.equal, true, action.limit)
// 		if getIdsErr != nil {
// 			log.Printf("Index.runQuery Greater: %s\n", getIdsErr.Error())
// 			return tmpIDs
// 		}
// 		ids = tmpIDs
// 	} else if action.GetType() == Less {
// 		tmpIDs, getIdsErr := i.getIDsFunc(action.ValueToCompareAsBytes(), action.equal, false, action.limit)
// 		if getIdsErr != nil {
// 			log.Printf("Index.runQuery Less: %s\n", getIdsErr.Error())
// 			return tmpIDs
// 		}
// 		ids = tmpIDs
// 	}

// 	return
// }

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
