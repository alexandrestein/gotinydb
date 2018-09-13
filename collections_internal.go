package gotinydb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/google/btree"
	"github.com/minio/highwayhash"
)

func (c *Collection) loadInfos() error {
	return c.store.View(func(txn *badger.Txn) error {
		id := c.buildIDWhitPrefixConfig([]byte("name"))
		item, err := txn.Get(id)
		if err != nil {
			return err
		}

		c.name = item.String()
		return nil
	})
}

// func (c *Collection) init(name string) error {
// 	return c.store.Update(func(txn *badger.Txn) error {
// 		id := c.buildIDWhitPrefixConfig([]byte("name"))
// 		return txn.Set(id, []byte(name))
// 	})
// }

func (c *Collection) getIndexesFromConfigBucket() []*indexType {
	indexes := []*indexType{}
	c.store.View(func(txn *badger.Txn) error {
		id := c.buildIDWhitPrefixConfig([]byte("indexesList"))
		indexesAsItem, err := txn.Get(id)
		if err != nil {
			return err
		}

		var indexesAsBytes []byte
		indexesAsBytes, err = indexesAsItem.Value()
		if err != nil {
			return err
		}

		json.Unmarshal(indexesAsBytes, &indexes)

		return nil
	})
	return indexes
}

// func (c *Collection) setIndexesIntoConfigBucket(index *indexType) error {
// 	return c.store.Update(func(tx *badger.Txn) error {
// 		id := c.buildIDWhitPrefixConfig([]byte("indexesList"))
// 		item, err := tx.Get(id)
// 		if err != nil {
// 			return err
// 		}

// 		var indexesAsBytes []byte
// 		indexesAsBytes, err = item.Value()

// 		indexes := []*indexType{}
// 		json.Unmarshal(indexesAsBytes, &indexes)

// 		indexes = append(indexes, index)

// 		indexesAsBytes, _ = json.Marshal(indexes)

// 		return tx.Set(id, indexesAsBytes)
// 	})
// }

func (c *Collection) buildCollectionPrefix() []byte {
	return []byte{c.prefix}
}
func (c *Collection) buildIDWhitPrefixData(id []byte) []byte {
	// prefixSpacer := make([]byte, 8)
	ret := []byte{c.prefix, prefixData}
	// ret = append(ret, prefixSpacer...)
	return append(ret, id...)
}
func (c *Collection) buildIDWhitPrefixConfig(id []byte) []byte {
	// prefixSpacer := make([]byte, 8)
	ret := []byte{c.prefix, prefixConfig}
	// ret = append(ret, prefixSpacer...)
	return append(ret, id...)
}
func (c *Collection) buildIDWhitPrefixIndex(indexName, id []byte) []byte {
	ret := []byte{c.prefix, prefixIndexes}

	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, highwayhash.Sum64(indexName, make([]byte, 32)))

	ret = append(ret, bs...)
	return append(ret, indexName...)
}
func (c *Collection) buildIDWhitPrefixRefs(id []byte) []byte {
	// prefixSpacer := make([]byte, 8)
	ret := []byte{c.prefix, prefixRefs}
	// ret = append(ret, prefixSpacer...)
	return append(ret, id...)
}

func (c *Collection) buildStoreID(id string) []byte {
	return c.buildIDWhitPrefixData([]byte(id))
}

func (c *Collection) putIntoIndexes(ctx context.Context, tx *badger.Txn, writeTransaction *writeTransactionElement) error {
	err := c.cleanRefs(ctx, tx, writeTransaction.id)
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return err
		}
	}

	refID := c.buildIDWhitPrefixRefs([]byte(writeTransaction.id))
	var refsAsBytes []byte

	item, err := tx.Get(refID)
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return err
		}
	} else {
		refsAsBytes, err = item.Value()
		if err != nil {
			return err
		}
	}

	refs := newRefs()
	if refsAsBytes != nil && len(refsAsBytes) > 0 {
		if err := json.Unmarshal(refsAsBytes, refs); err != nil {
			return err
		}
	}

	if refs.ObjectID == "" {
		refs.ObjectID = writeTransaction.id
	}
	// if refs.ObjectHashID == "" {
	// 	refs.ObjectHashID = buildID(writeTransaction.id)
	// }

	for _, index := range c.indexes {
		if indexedValues, apply := index.apply(writeTransaction.contentInterface); apply {
			// indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(index.Name))

			// If the selector hit a slice.
			// apply can returns more than one value to index
			for _, indexedValue := range indexedValues {
				var ids = new(idsType)

				indexedValueID := append(index.getIDBuilder(nil), indexedValue...)
				// Try to get the ids related to the field value
				idsAsItem, err := tx.Get(indexedValueID)
				if err != nil {
					if err != badger.ErrKeyNotFound {
						return err
					}
				} else {
					// If the list of ids is present for this index field value,
					// this save the actual status of the given filed value.
					var idsAsBytes []byte
					idsAsBytes, err = idsAsItem.Value()
					if err != nil {
						return err
					}

					ids, err = newIDs(ctx, 0, nil, idsAsBytes)
					if err != nil {
						return err
					}
				}

				id := newID(ctx, writeTransaction.id)
				ids.AddID(id)
				idsAsBytes := ids.MustMarshal()

				// Add the list of ID for the given field value
				e := &badger.Entry{
					Key:   indexedValueID,
					Value: idsAsBytes,
				}

				copyOfKey := make([]byte, len(e.Key))
				copy(copyOfKey, e.Key)
				copyOfValue := make([]byte, len(e.Value))
				copy(copyOfValue, e.Value)
				e.Key = copyOfKey
				e.Value = copyOfValue

				fmt.Println(1, "key:", e.Key, "valAsString:", string(e.Value))
				if err := tx.SetEntry(e); err != nil {
					// if err := tx.Set(indexedValueID, idsAsBytes); err != nil {
					return err
				}

				// Update the object references at the memory level
				refs.setIndexedValue(index.Name, index.selectorHash(), indexedValue)
			}
		}
	}

	// Save the new reference stat on persistant storage
	e := &badger.Entry{
		Key:   refID,
		Value: refs.asBytes(),
	}
	fmt.Println(2, "key:", e.Key, "valAsString:", string(e.Value))

	copyOfKey := make([]byte, len(e.Key))
	copy(copyOfKey, e.Key)
	copyOfValue := make([]byte, len(e.Value))
	copy(copyOfValue, e.Value)
	e.Key = copyOfKey
	e.Value = copyOfValue

	return tx.SetEntry(e)
	// return tx.Set(refID, refs.asBytes())

	// putErr := tx.Set(refID, refs.asBytes())
	// if putErr != nil {
	// 	return err
	// }

	// return c.endOfIndexUpdate(ctx, tx, !multi)
}

// func (c *Collection) onlyCleanRefs(ctx context.Context, tx *badger.Txn, writeTransaction *writeTransactionElement) error {
// 	return c.cleanRefs(ctx, tx, writeTransaction.id)
// 	// err := c.cleanRefs(ctx, tx, writeTransaction.id)
// 	// if err != nil {
// 	// 	return err
// 	// }

// 	// return c.endOfIndexUpdate(ctx, tx, !multi)
// }

// func (c *Collection) endOfIndexUpdate(ctx context.Context, tx *badger.Txn, commit bool) error {
// 	if commit {
// 		return tx.Commit(nil)
// 	}
// 	return nil
// }

func (c *Collection) cleanRefs(ctx context.Context, tx *badger.Txn, idAsString string) error {
	// indexBucket := tx.Bucket([]byte("indexes"))
	// refsBucket := tx.Bucket([]byte("refs"))

	var refsAsBytes []byte

	// Get the references of the given ID
	refsDBID := c.buildIDWhitPrefixRefs([]byte(idAsString))
	refsAsItem, err := tx.Get(refsDBID)
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return err
		}
	} else {
		refsAsBytes, err = refsAsItem.Value()
		if err != nil {
			return err
		}
	}

	refs := newRefs()
	if refsAsBytes != nil && len(refsAsBytes) > 0 {
		if err := json.Unmarshal(refsAsBytes, refs); err != nil {
			return err
		}
	}

	// Clean every reference of the object In all indexes if present
	for _, ref := range refs.Refs {
		for _, index := range c.indexes {
			if index.Name == ref.IndexName {
				fmt.Println("clean", index.Name)
				indexIDForTheGivenObjectAsBytes := c.buildIDWhitPrefixIndex([]byte(index.Name), ref.IndexedValue)
				indexedValueAsItem, err := tx.Get(indexIDForTheGivenObjectAsBytes)
				if err != nil {
					return err
				}
				var indexedValueAsBytes []byte
				indexedValueAsBytes, err = indexedValueAsItem.Value()
				if err != nil {
					return err
				}
				// If reference present in this index the reference is cleaned
				ids, newIDErr := newIDs(ctx, 0, nil, indexedValueAsBytes)
				if newIDErr != nil {
					return newIDErr
				}
				ids.RmID(idAsString)

				// And saved again after the clean
				e := &badger.Entry{
					Key:   indexIDForTheGivenObjectAsBytes,
					Value: ids.MustMarshal(),
				}
				fmt.Println(3, "key:", e.Key, "valAsString:", string(e.Value))

				copyOfKey := make([]byte, len(e.Key))
				copy(copyOfKey, e.Key)
				copyOfValue := make([]byte, len(e.Value))
				copy(copyOfValue, e.Value)
				e.Key = copyOfKey
				e.Value = copyOfValue

				err = tx.SetEntry(e)
				// err = tx.Set(indexIDForTheGivenObjectAsBytes, ids.MustMarshal())
				if err != nil {
					return err
				}
				fmt.Println("end clean")
			}
		}
	}

	refsAsBytes, err = json.Marshal(refs)
	if err != nil {
		return err
	}

	e := &badger.Entry{
		Key:   refsDBID,
		Value: refsAsBytes,
	}
	fmt.Println(4, "key:", e.Key, "valAsString:", string(e.Value))

	copyOfKey := make([]byte, len(e.Key))
	copy(copyOfKey, e.Key)
	copyOfValue := make([]byte, len(e.Value))
	copy(copyOfValue, e.Value)
	e.Key = copyOfKey
	e.Value = copyOfValue

	return tx.SetEntry(e)
	// return tx.Set(refsDBID, refsAsBytes)
}

func (c *Collection) queryGetIDs(ctx context.Context, q *Query) (*btree.BTree, error) {
	// Init the destination
	tree := btree.New(10)

	// Initialize the channel which will confirm that all queries are done
	finishedChan := make(chan *idsType, 16)
	defer close(finishedChan)

	excludeFinishedChan := make(chan *idsType, 16)
	defer close(excludeFinishedChan)

	// This count the number of running index query for this actual collection query
	nbToDo := 0

	// Goes through all index of the collection to define which index
	// will take care of the given filter
	for _, index := range c.indexes {
		for _, filterInterface := range q.filters {
			filter := filterInterface.getFilterBase()
			if index.doesFilterApplyToIndex(filter) {
				if !filter.exclusion {
					go index.query(ctx, filter, finishedChan)
				} else {
					go index.query(ctx, filter, excludeFinishedChan)
				}
				nbToDo++
			}
		}
	}

	if nbToDo == 0 {
		return nil, ErrIndexNotFound
	}

	// Loop every response from the index query
	return c.queryGetIDsLoop(ctx, tree, finishedChan, excludeFinishedChan, nbToDo)
}

func (c *Collection) queryGetIDsLoop(ctx context.Context, tree *btree.BTree, finishedChan, excludeFinishedChan chan *idsType, nbToDo int) (*btree.BTree, error) {
	for {
		select {
		case tmpIDs := <-finishedChan:
			if tmpIDs != nil {
				// Add IDs into the response tree
				for _, id := range tmpIDs.IDs {
					c.queryGetIDsLoopIncrementFuncfunc(tree, id, 1)
				}
			}
		case tmpIDs := <-excludeFinishedChan:
			if tmpIDs != nil {
				// Add IDs into the response tree
				for _, id := range tmpIDs.IDs {
					c.queryGetIDsLoopIncrementFuncfunc(tree, id, -1)
				}
			}
		case <-ctx.Done():
			time.Sleep(time.Millisecond * 100)
			return nil, ErrTimeOut
		}

		// Save the fact that one more query has respond
		nbToDo--
		// If nomore query to wait, quit the loop
		if nbToDo <= 0 {
			if tree.Len() == 0 {
				return nil, ErrNotFound
			}
			return tree, nil
		}
	}
}

func (c *Collection) queryGetIDsLoopIncrementFuncfunc(tree *btree.BTree, id *idType, nb int) {
	// Try to get the id from the tree
	fromTree := tree.Get(id)
	if fromTree == nil {
		// If not in the tree add it
		id.Increment(nb)
		tree.ReplaceOrInsert(id)
		return
	}
	// if already increment the counter
	fromTree.(*idType).Increment(nb)
}

func (c *Collection) queryCleanAndOrder(ctx context.Context, q *Query, tree *btree.BTree) (response *Response, _ error) {
	getRefFunc := func(id string) (refs *refs) {
		c.store.View(func(tx *badger.Txn) error {
			refs, _ = c.getRefs(tx, id)
			return nil
		})
		return refs
	}

	// iterate the response tree to get only IDs which has been found in every index queries
	occurrenceFunc, idsSlice := occurrenceTreeIterator(q.nbSelectFilters(), q.internalLimit, q.order, getRefFunc)
	tree.Ascend(occurrenceFunc)

	// Build the new sorter
	idsMs := new(idsTypeMultiSorter)
	idsMs.IDs = idsSlice.IDs

	// Invert the sort order
	if !q.ascendent {
		idsMs.invert = true
	}

	// Do the sorting
	idsMs.Sort(q.limit)

	// Build the response for the caller
	response = newResponse(len(idsMs.IDs))
	response.query = q

	// Get every content of the query from the database
	responsesAsBytes, err := c.get(ctx, getIDsAsString(idsSlice.IDs)...)
	if err != nil {
		return nil, err
	}

	// Range the response values as slice of bytes
	for i := range responsesAsBytes {
		if i >= q.limit {
			break
		}

		response.list[i] = &ResponseElem{
			_ID:            idsSlice.IDs[i],
			contentAsBytes: responsesAsBytes[i],
		}
	}
	return
}

func (c *Collection) insertOrDeleteStore(ctx context.Context, txn *badger.Txn, isInsertion bool, writeTransaction *writeTransactionElement) error {
	hashSignature, _ := uintToBytes((highwayhash.Sum64(writeTransaction.contentAsBytes, make([]byte, highwayhash.Size))))
	contentToWrite := append(hashSignature, writeTransaction.contentAsBytes...)

	storeID := c.buildStoreID(writeTransaction.id)

	var writeErr error
	if isInsertion {
		e := &badger.Entry{
			Key:   storeID,
			Value: contentToWrite,
		}
		fmt.Println(5, "key:", e.Key, "valAsString:", string(e.Value))

		copyOfKey := make([]byte, len(e.Key))
		copy(copyOfKey, e.Key)
		copyOfValue := make([]byte, len(e.Value))
		copy(copyOfValue, e.Value)
		e.Key = copyOfKey
		e.Value = copyOfValue

		writeErr = txn.SetEntry(e)
		// writeErr = txn.Set(storeID, contentToWrite)
	} else {
		writeErr = txn.Delete(storeID)
	}
	if writeErr != nil {
		return fmt.Errorf("error writing %q: %s", writeTransaction.id, writeErr.Error())
	}

	return nil
}

// func (c *Collection) putIntoStore(ctx context.Context, txn *badger.Txn, writeTransaction *writeTransactionElement) error {
// 	return c.insertOrDeleteStore(ctx, txn, true, writeTransaction)
// }

// func (c *Collection) delFromStore(ctx context.Context, txn *badger.Txn, writeTransaction *writeTransactionElement) error {
// 	return c.insertOrDeleteStore(ctx, txn, false, writeTransaction)
// }

func (c *Collection) get(ctx context.Context, ids ...string) ([][]byte, error) {
	ret := make([][]byte, len(ids))
	if err := c.store.View(func(txn *badger.Txn) error {
		for i, id := range ids {
			idAsBytes := c.buildStoreID(id)
			item, getError := txn.Get(idAsBytes)
			if getError != nil {
				if getError == badger.ErrKeyNotFound {
					return ErrNotFound
				}
				return getError
			}

			if item.IsDeletedOrExpired() {
				return ErrNotFound
			}

			contentAndHashSignatureAsBytes, getValErr := item.Value()
			if getValErr != nil {
				return getValErr
			}

			contentAsBytes, corrupted := c.getAndCheckContent(contentAndHashSignatureAsBytes)
			if corrupted != nil {
				return corrupted
			}

			ret[i] = contentAsBytes
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Collection) getAndCheckContent(contentAndHashSignatureAsBytes []byte) (content []byte, _ error) {
	if len(contentAndHashSignatureAsBytes) <= 8 {
		return nil, ErrDataCorrupted
	}

	savedSignature := contentAndHashSignatureAsBytes[:8]
	contentAsBytes := contentAndHashSignatureAsBytes[8:]
	retrievedSignature, _ := uintToBytes(highwayhash.Sum64(contentAsBytes, make([]byte, highwayhash.Size)))

	if !reflect.DeepEqual(savedSignature, retrievedSignature) {
		return nil, ErrDataCorrupted
	}

	return contentAsBytes, nil
}

// func (c *Collection) loadIndex() error {
// 	indexes := c.getIndexesFromConfigBucket()
// 	for _, index := range indexes {
// 		index.options = c.options
// 		index.getTx = c.store.NewTransaction
// 		index.getIDBuilder = func(id []byte) []byte {
// 			return c.buildIDWhitPrefixIndex([]byte(index.Name), id)
// 		}
// 	}
// 	c.indexes = indexes

// 	return nil
// }

func (c *Collection) getRefs(tx *badger.Txn, id string) (*refs, error) {
	refsAsItem, err := tx.Get(c.buildIDWhitPrefixRefs([]byte(id)))
	if err != nil {
		return nil, err
	}
	var refsAsBytes []byte
	refsAsBytes, err = refsAsItem.Value()
	if err != nil {
		return nil, err
	}
	refs := newRefsFromDB(refsAsBytes)
	if refs == nil {
		return nil, fmt.Errorf("references mal formed: %s", string(refsAsBytes))
	}
	return refs, nil
}

// getStoredIDs returns all ids if it does not exceed the limit.
// This will not returned the ID used to set the value inside the collection
// It returns the id used to set the value inside the store
func (c *Collection) getStoredIDsAndValues(starter string, limit int, IDsOnly bool) ([]*ResponseElem, error) {
	response := make([]*ResponseElem, limit)

	err := c.store.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := c.buildIDWhitPrefixData(nil)
		iter.Seek(c.buildIDWhitPrefixData([]byte(starter)))

		count := 0
		for ; iter.Valid(); iter.Next() {
			if !iter.ValidForPrefix(prefix) || count > limit-1 {
				response = response[:count]
				return nil
			}

			responseItem := new(ResponseElem)

			item := iter.Item()

			if item.IsDeletedOrExpired() {
				continue
			}

			responseItem._ID = new(idType)
			responseItem._ID.ID = string(item.Key()[len(c.buildIDWhitPrefixData(nil)):])

			if !IDsOnly {
				var err error
				responseItem.contentAsBytes, err = item.ValueCopy(responseItem.contentAsBytes)
				if err != nil {
					return err
				}

				var corrupted error
				responseItem.contentAsBytes, corrupted = c.getAndCheckContent(responseItem.contentAsBytes)
				if corrupted != nil {
					return corrupted
				}
			}

			response[count] = responseItem

			count++
		}

		// Clean the end of the slice if not full
		response = response[:count]
		return nil
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *Collection) indexAllValues() error {
	// func (c *Collection) indexAllValues(i *indexType) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lastID := ""

newLoop:
	savedElements, getErr := c.getStoredIDsAndValues(lastID, c.options.PutBufferLimit, false)
	if getErr != nil {
		return getErr
	}

	if len(savedElements) <= 1 {
		return nil
	}

	tx := c.store.NewTransaction(true)
	defer tx.Discard()

	for _, savedElement := range savedElements {
		if savedElement.GetID() == lastID {
			continue
		}

		var elem interface{}
		decoder := json.NewDecoder(bytes.NewBuffer(savedElement.contentAsBytes))
		decoder.UseNumber()

		if jsonErr := decoder.Decode(&elem); jsonErr != nil {
			return jsonErr
		}

		m := elem.(map[string]interface{})

		ctx2, cancel2 := context.WithTimeout(ctx, c.options.TransactionTimeOut)
		defer cancel2()

		trElement := newTransactionElement(savedElement.GetID(), m, true, c)

		err := c.putIntoIndexes(ctx2, tx, trElement)
		if err != nil {
			return err
		}

		lastID = savedElement.GetID()
	}

	err := tx.Commit(nil)
	if err != nil {
		return err
	}

	goto newLoop
}

func (c *Collection) isRunning() bool {
	if c.ctx.Err() != nil {
		return false
	}

	return true
}
