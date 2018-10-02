package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"
	"github.com/google/btree"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transactions"
)

func (c *Collection) getCollectionPrefix() []byte {
	return []byte{prefixCollections, c.prefix}
}
func (c *Collection) buildCollectionPrefix(nextPrefix byte) []byte {
	return append(c.getCollectionPrefix(), nextPrefix)
}
func (c *Collection) buildIDWhitPrefixData(id []byte) []byte {
	ret := c.buildCollectionPrefix(prefixData)
	return append(ret, id...)
}
func (c *Collection) buildIDWhitPrefixIndex(indexName, id []byte) []byte {
	ret := append(c.buildCollectionPrefix(prefixIndexes), deriveName(indexName, 8)...)
	return append(ret, id...)
}
func (c *Collection) buildIDWhitPrefixBleveIndex(indexName, id []byte) []byte {
	ret := append(c.buildCollectionPrefix(prefixBleveIndexes), deriveName(indexName, 8)...)
	return append(ret, id...)
}
func (c *Collection) buildIDWhitPrefixRefs(id []byte) []byte {
	ret := c.buildCollectionPrefix(prefixRefs)
	return append(ret, id...)
}

func (c *Collection) buildStoreID(id string) []byte {
	return c.buildIDWhitPrefixData([]byte(id))
}

func (c *Collection) initBleveIndexes() error {
	for _, i := range c.bleveIndexes {
		i.kvConfig = c.buildKvConfig(i.IndexPrefix)
		err := i.open()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Collection) buildKvConfig(indexPrefix []byte) (config map[string]interface{}) {
	return map[string]interface{}{
		"path": "test",
		"config": blevestore.NewBleveStoreConfig(
			[32]byte{},
			indexPrefix,
			c.store,
			c.writeTransactionChan,
		),
	}
}

func (c *Collection) putIntoIndexes(ctx context.Context, txn *badger.Txn, writeElem *transactions.WriteElement) error {
	// err := c.cleanRefs(ctx, txn, writeElem.CollectionID)
	// if err != nil {
	// 	if err != badger.ErrKeyNotFound {
	// 		return err
	// 	}
	// }

	// refID := c.buildIDWhitPrefixRefs([]byte(writeElem.CollectionID))

	// refs := newRefs()

	// if refs.ObjectID == "" {
	// 	refs.ObjectID = writeElem.CollectionID
	// }

	// for _, index := range c.indexes {
	// 	if indexedValues, apply := index.apply(writeTransaction.contentInterface); apply {
	// 		// If the selector hit a slice.
	// 		// apply can returns more than one value to index
	// 		for _, indexedValue := range indexedValues {
	// 			var ids = new(idsType)

	// 			indexedValueID := append(index.getIDBuilder(nil), indexedValue...)
	// 			// Try to get the ids related to the field value
	// 			idsAsItem, err := txn.Get(indexedValueID)
	// 			if err != nil {
	// 				if err != badger.ErrKeyNotFound {
	// 					return err
	// 				}
	// 			} else {
	// 				// If the list of ids is present for this index field value,
	// 				// this save the actual status of the given filed value.
	// 				var idsAsBytesEncrypted []byte
	// 				idsAsBytesEncrypted, err = idsAsItem.ValueCopy(idsAsBytesEncrypted)
	// 				if err != nil {
	// 					return err
	// 				}

	// 				// Decrypt value
	// 				var idsAsBytes []byte
	// 				idsAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, idsAsItem.Key(), idsAsBytesEncrypted)
	// 				if err != nil {
	// 					return err
	// 				}

	// 				ids, _ = newIDs(ctx, 0, nil, idsAsBytes)
	// 			}

	// 			id := newID(ctx, writeTransaction.id)
	// 			ids.AddID(id)
	// 			idsAsBytes := ids.MustMarshal()

	// 			// Add the list of ID for the given field value
	// 			e := &badger.Entry{
	// 				Key:   indexedValueID,
	// 				Value: cipher.Encrypt(c.options.privateCryptoKey, indexedValueID, idsAsBytes),
	// 			}

	// 			if err := txn.SetEntry(e); err != nil {
	// 				return err
	// 			}

	// 			// Update the object references at the memory level
	// 			refs.setIndexedValue(index.Name, index.selectorHash(), indexedValue)
	// 		}
	// 	}
	// }

	// for _, index := range c.bleveIndexes {
	// 	if indexedValues, apply := index.Selector.apply(writeTransaction.contentInterface); apply {
	// 		var err error
	// 		index, err = c.getBleveIndex(index.Name)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		err = index.index.Index(writeTransaction.id, indexedValues)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	// // Save the new reference stat on persistent storage
	// e := &badger.Entry{
	// 	Key:   refID,
	// 	Value: cipher.Encrypt(c.options.privateCryptoKey, refID, refs.asBytes()),
	// }

	// return txn.SetEntry(e)

	fmt.Println("putIntoIndex")
	return nil
}

func (c *Collection) cleanRefs(ctx context.Context, txn *badger.Txn, idAsString string) error {
	// var refsAsBytes []byte

	// // Get the references of the given ID
	// refsDbID := c.buildIDWhitPrefixRefs([]byte(idAsString))
	// refsAsItem, err := txn.Get(refsDbID)
	// if err != nil {
	// 	if err != badger.ErrKeyNotFound {
	// 		return err
	// 	}
	// } else {
	// 	var refsAsEncryptedBytes []byte
	// 	refsAsEncryptedBytes, err = refsAsItem.ValueCopy(refsAsEncryptedBytes)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	refsAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, refsAsItem.Key(), refsAsEncryptedBytes)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// refs := newRefs()
	// if refsAsBytes != nil && len(refsAsBytes) > 0 {
	// 	json.Unmarshal(refsAsBytes, refs)
	// }

	// // Clean every reference of the object In all indexes if present
	// for _, ref := range refs.Refs {
	// 	for _, index := range c.indexes {
	// 		if index.Name == ref.IndexName {
	// 			indexIDForTheGivenObjectAsBytes := c.buildIDWhitPrefixIndex([]byte(index.Name), ref.IndexedValue)
	// 			indexedValueAsItem, err := txn.Get(indexIDForTheGivenObjectAsBytes)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			var indexedValueAsEncryptedBytes []byte
	// 			indexedValueAsEncryptedBytes, err = indexedValueAsItem.ValueCopy(indexedValueAsEncryptedBytes)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			var indexedValueAsBytes []byte
	// 			indexedValueAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, indexedValueAsItem.Key(), indexedValueAsEncryptedBytes)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			// If reference present in this index the reference is cleaned
	// 			ids, _ := newIDs(ctx, 0, nil, indexedValueAsBytes)
	// 			ids.RmID(idAsString)

	// 			// And saved again after the clean
	// 			e := &badger.Entry{
	// 				Key:   indexIDForTheGivenObjectAsBytes,
	// 				Value: cipher.Encrypt(c.options.privateCryptoKey, indexIDForTheGivenObjectAsBytes, ids.MustMarshal()),
	// 			}

	// 			err = txn.SetEntry(e)
	// 			if err != nil {
	// 				return err
	// 			}
	// 		}
	// 	}
	// }

	// refsAsBytes, _ = json.Marshal(refs)

	// e := &badger.Entry{
	// 	Key:   refsDbID,
	// 	Value: cipher.Encrypt(c.options.privateCryptoKey, refsDbID, refsAsBytes),
	// }

	// return txn.SetEntry(e)

	fmt.Println("cleanRefs")
	return nil
}

func (c *Collection) cleanFromBleve(ctx context.Context, txn *badger.Txn, idAsString string) error {
	for _, bleveIndex := range c.bleveIndexes {
		err := bleveIndex.index.Delete(idAsString)
		if err != nil {
			return err
		}
	}

	return nil
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
		for _, filter := range q.filters {
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

	// Iterate the response tree to get only IDs which has been found in every index queries.
	// The response goes to idsSlice.
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

func (c *Collection) insertOrDeleteStore(ctx context.Context, txn *badger.Txn, isInsertion bool, writeTransaction *transactions.WriteTransaction) error {
	fmt.Println("insertOrDeleteStore")
	return nil
	// storeID := c.buildStoreID(writeTransaction.id)

	// if isInsertion {
	// 	e := &badger.Entry{
	// 		Key:   storeID,
	// 		Value: cipher.Encrypt(c.options.privateCryptoKey, storeID, writeTransaction.contentAsBytes),
	// 	}

	// 	return txn.SetEntry(e)
	// }
	// return txn.Delete(storeID)
}

func (c *Collection) get(ctx context.Context, ids ...string) ([][]byte, error) {
	ret := make([][]byte, len(ids))
	return ret, c.store.View(func(txn *badger.Txn) error {
		for i, id := range ids {
			idAsBytes := c.buildStoreID(id)
			var err error
			var item *badger.Item
			item, err = txn.Get(idAsBytes)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return ErrNotFound
				}
				return err
			}

			if item.IsDeletedOrExpired() {
				return ErrNotFound
			}

			var contentAsEncryptedBytes []byte
			contentAsEncryptedBytes, err = item.ValueCopy(contentAsEncryptedBytes)
			if err != nil {
				return err
			}

			var contentAsBytes []byte
			contentAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, item.Key(), contentAsEncryptedBytes)
			if err != nil {
				return err
			}

			ret[i] = contentAsBytes
		}
		return nil
	})
}

func (c *Collection) getRefs(txn *badger.Txn, id string) (*refs, error) {
	refsAsItem, err := txn.Get(c.buildIDWhitPrefixRefs([]byte(id)))
	if err != nil {
		return nil, err
	}
	var refsAsEncryptedBytes []byte
	refsAsEncryptedBytes, err = refsAsItem.ValueCopy(refsAsEncryptedBytes)
	if err != nil {
		return nil, err
	}
	var refsAsBytes []byte
	refsAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, refsAsItem.Key(), refsAsEncryptedBytes)
	if err != nil {
		return nil, err
	}
	refs := newRefsFromDB(refsAsBytes)
	return refs, nil
}

// getStoredIDs returns all ids if it does not exceed the limit.
// This will not returned the ID used to set the value inside the collection
// It returns the id used to set the value inside the store
func (c *Collection) getStoredIDsAndValues(starter string, limit int, IDsOnly bool) ([]*ResponseElem, error) {
	response := make([]*ResponseElem, limit)

	return response, c.store.View(func(txn *badger.Txn) error {
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

				responseItem.contentAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, item.Key(), responseItem.contentAsBytes)
				if err != nil {
					return err
				}
			}

			response[count] = responseItem

			count++
		}

		// Clean the end of the slice if not full
		response = response[:count]
		return nil
	})
}

func (c *Collection) indexAllValues() error {
	lastID := ""

newLoop:
	savedElements, getErr := c.getStoredIDsAndValues(lastID, c.options.PutBufferLimit, false)
	if getErr != nil {
		return getErr
	}

	if len(savedElements) <= 1 {
		return nil
	}

	txn := c.store.NewTransaction(true)
	defer txn.Discard()

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

		ctx, cancel := context.WithTimeout(c.ctx, c.options.TransactionTimeOut)
		defer cancel()

		fmt.Println("id is not valid", savedElement.GetID())
		trElement := transactions.NewTransactionElement([]byte(savedElement.GetID()), savedElement.GetContent())
		// trElement := newTransactionElement(savedElement.GetID(), m, true, c)

		err := c.putIntoIndexes(ctx, txn, trElement)
		if err != nil {
			return err
		}

		lastID = savedElement.GetID()
	}

	err := txn.Commit(nil)
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

func (c *Collection) getBleveIndex(name string) (*bleveIndex, error) {
	var index *bleveIndex

	// Loop all indexes to found the given index
	found := false
	for _, i := range c.bleveIndexes {
		if i.Name == name {
			index = i
			found = true
			break
		}
	}

	if !found {
		return nil, ErrIndexNotFound
	}

	// If index is already loaded
	if index.index != nil {
		return index, nil
	}

	// Load the index
	bleveIndex, err := bleve.OpenUsing(index.Path, c.buildKvConfig(index.IndexPrefix))
	if err != nil {
		return nil, err
	}

	// Save the index interface into the internal index type
	index.index = bleveIndex

	return index, nil
}
