package collection

// func (i *structIndex) RunQuery(q *query.Query) (ids []string) {
// 	if q == nil {
// 		return
// 	}
//
// 	if !i.doesApply(q.Selector) {
// 		return
// 	}
//
// 	// Actualy run the query
// 	ids = i.runQuery(q, true)
// 	if q.KeepAction != nil && len(ids) != 0 {
// 		ids = i.runKeepQuery(q, ids)
// 	}
//
// 	if q.Distinct {
// 		keys := make(map[string]bool)
// 		list := []string{}
// 		for _, id := range ids {
// 			if _, value := keys[id]; !value {
// 				keys[id] = true
// 				list = append(list, id)
// 			}
// 		}
// 	}
//
// 	// Cleans the list if to big and returns
// 	if len(ids) > q.Limit {
// 		ids = ids[:q.Limit]
// 		return
// 	}
//
// 	// Reverts the result if wanted
// 	if q.InvertedOrder {
// 		for i := len(ids)/2 - 1; i >= 0; i-- {
// 			opp := len(ids) - 1 - i
// 			ids[i], ids[opp] = ids[opp], ids[i]
// 		}
// 	}
// 	return
// }
//
// func (i *structIndex) runQuery(q *query.Query, get bool) (ids []string) {
// 	var action *query.Action
//
// 	// If query is not nil
// 	if q == nil {
// 		return
// 	}
//
// 	// If the caller want to do the get lockup
// 	if get {
// 		// If the get action is nil it returns
// 		if q.GetAction == nil {
// 			return
// 		}
// 		// Otherways it save the action
// 		action = q.GetAction
// 		// The caller do not asked for a get action and the saved action is keep
// 		// if any
// 	} else {
// 		if q.KeepAction == nil {
// 			return
// 		}
// 		action = q.KeepAction
// 	}
//
// 	if action.CompareToValue == nil {
// 		return
// 	}
//
// 	// If equal just this leave will be send
// 	if action.GetType() == query.Equal {
// 		tmpIDs, found := i.Get(action.CompareToValue)
// 		if found {
// 			ids = tmpIDs
// 			if len(ids) > q.Limit {
// 				ids = ids[:q.Limit]
// 			}
// 		}
// 		return
// 	}
//
// 	var iterator btree.Iterator
// 	var iteratorInit bool
// 	var nextFunc (func() bool)
// 	var keyFound bool
//
// 	if action.GetType() == query.Greater {
// 		_, keyAfter, found := i.tree.GetClosestKeys(action.CompareToValue)
// 		keyFound = found
// 		if keyAfter != nil {
// 			iterator, _ = i.tree.IteratorAt(keyAfter)
// 			iteratorInit = true
// 		}
// 		nextFunc = iterator.Next
// 	} else if action.GetType() == query.Less {
// 		keyBefore, _, found := i.tree.GetClosestKeys(action.CompareToValue)
// 		keyFound = found
// 		if keyBefore != nil {
// 			iterator, _ = i.tree.IteratorAt(keyBefore)
// 			iteratorInit = true
// 		}
// 		nextFunc = iterator.Prev
// 	}
//
// 	// Check if the caller want more or less with equal option
// 	if keyFound {
// 		if !q.KeepEqual {
// 			ids = append(ids, iterator.Value().([]string)...)
// 		}
// 	} else {
// 		if iteratorInit {
// 			ids = append(ids, iterator.Value().([]string)...)
// 		}
// 	}
//
// 	if !iteratorInit {
// 		return
// 	}
//
// 	for nextFunc() {
// 		ids = append(ids, iterator.Value().([]string)...)
// 	}
// 	return
// }
//
// func (i *structIndex) runKeepQuery(q *query.Query, ids []string) []string {
// 	// Do the lock up for the IDs to remove
// 	idsToRemove := i.runQuery(q, false)
// 	indexToRemove := []int{}
//
// 	// Do the check of which IDs need to be removed
// 	for j, id1 := range ids {
// 		for _, id2 := range idsToRemove {
// 			if id1 == id2 {
// 				// Build the list of ids to remove
// 				indexToRemove = append(indexToRemove, j)
// 			}
// 		}
// 	}
//
// 	// Once we know which index we need to remove we start at the end of the slice
// 	// and we remove every not needed IDs.
// 	for j := len(indexToRemove) - 1; j >= 0; j-- {
// 		if len(ids) > indexToRemove[j] {
// 			ids = append(ids[:indexToRemove[j]], ids[indexToRemove[j]+1:]...)
// 		} else {
// 			ids = ids[:indexToRemove[j]]
// 		}
// 	}
//
// 	return ids
// }
