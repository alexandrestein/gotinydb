package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
	"github.com/google/btree"
)

func (c *Collection) loadInfos() error {
	return c.db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("config"))
		if bucket == nil {
			return ErrNotFound
		}

		name := string(bucket.Get([]byte("name")))
		c.name = name

		return nil
	})
}

func (c *Collection) init(name string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucketsToCreate := []string{"config", "indexes", "refs"}
		for _, bucketName := range bucketsToCreate {
			if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
				return err
			}
		}

		confBucket := tx.Bucket([]byte("config"))
		if confBucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		if nameErr := confBucket.Put([]byte("name"), []byte(name)); nameErr != nil {
			return nameErr
		}
		return nil
	})
}

func (c *Collection) getIndexesFromConfigBucket() []*indexType {
	indexes := []*indexType{}
	c.db.View(func(tx *bolt.Tx) error {
		indexesAsBytes := tx.Bucket([]byte("config")).Get([]byte("indexesList"))
		json.Unmarshal(indexesAsBytes, &indexes)

		return nil
	})
	return indexes
}

func (c *Collection) setIndexesIntoConfigBucket(index *indexType) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		confBucket := tx.Bucket([]byte("config"))
		indexesAsBytes := confBucket.Get([]byte("indexesList"))
		indexes := []*indexType{}
		json.Unmarshal(indexesAsBytes, &indexes)

		found := false
		for i, tmpIndex := range indexes {
			if tmpIndex.Name == index.Name {
				indexes[i] = index
				found = true
				break
			}
		}
		if !found {
			indexes = append(indexes, index)
		}

		indexesAsBytes, _ = json.Marshal(indexes)
		return confBucket.Put([]byte("indexesList"), indexesAsBytes)
	})
}
func (c *Collection) delIndexesIntoConfigBucket(indexName string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		confBucket := tx.Bucket([]byte("config"))
		indexesAsBytes := confBucket.Get([]byte("indexesList"))
		indexes := []*indexType{}
		err := json.Unmarshal(indexesAsBytes, &indexes)
		if err != nil {
			return err
		}

		for i, index := range indexes {
			if index.Name == indexName {
				copy(indexes[i:], indexes[i+1:])
				indexes[len(indexes)-1] = nil
				indexes = indexes[:len(indexes)-1]
				break
			}
		}

		indexesAsBytes, _ = json.Marshal(indexes)
		return confBucket.Put([]byte("indexesList"), indexesAsBytes)
	})
}

func (c *Collection) initWriteTransactionChan(ctx context.Context) {
	c.writeTransactionChan = make(chan *writeTransaction, 1000)
	go func() {
		for {
			select {
			case tr := <-c.writeTransactionChan:
				c.putTransaction(tr)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Collection) putTransaction(tr *writeTransaction) {
	// Build a waiting groups
	// This group is to make internal functions wait the otherone
	wgActions := new(sync.WaitGroup)
	// This group defines the waitgroup to consider that all have been done correctly.
	wgCommitted := new(sync.WaitGroup)

	// Used to propagate the error for one or the other function
	errChan := make(chan error, 1)

	// Increment the tow waiting groups
	wgActions.Add(2)
	wgCommitted.Add(2)

	// Runs saving into the store
	go c.putIntoStore(tr.ctx, errChan, wgActions, wgCommitted, tr)

	// Starts the indexing process
	if !tr.bin {
		go c.putIntoIndexes(tr.ctx, errChan, wgActions, wgCommitted, tr)
	} else {
		go c.onlyCleanRefs(tr.ctx, errChan, wgActions, wgCommitted, tr)
	}

	// Respond to the caller with the error if any
	tr.responseChan <- waitForDoneErrOrCanceled(tr.ctx, wgCommitted, errChan)
}

func (c *Collection) buildStoreID(id string) []byte {
	return []byte(fmt.Sprintf("%s_%s", c.id[:4], id))
}

func (c *Collection) putIntoIndexes(ctx context.Context, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransaction) error {
	tx, txErr := c.db.Begin(true)
	if txErr != nil {
		errChan <- txErr
		return txErr
	}
	// return c.db.Update(func(tx *bolt.Tx) error {
	err := c.cleanRefs(ctx, tx, writeTransaction.id)
	if err != nil {
		errChan <- err
		return err
	}

	refsBucket := tx.Bucket([]byte("refs"))
	refsAsBytes := refsBucket.Get(buildBytesID(writeTransaction.id))
	refs := newRefs()
	if refsAsBytes != nil && len(refsAsBytes) > 0 {
		if err := json.Unmarshal(refsAsBytes, refs); err != nil {
			errChan <- err
			return err
		}
	}

	if refs.ObjectID == "" {
		refs.ObjectID = writeTransaction.id
	}
	if refs.ObjectHashID == "" {
		refs.ObjectHashID = buildID(writeTransaction.id)
	}

	for _, index := range c.indexes {
		if indexedValue, apply := index.apply(writeTransaction.contentInterface); apply {
			indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(index.Name))

			idsAsBytes := indexBucket.Get(indexedValue)
			ids, parseIDsErr := newIDs(ctx, 0, nil, idsAsBytes)
			if parseIDsErr != nil {
				errChan <- parseIDsErr
				return parseIDsErr
			}

			id := newID(ctx, writeTransaction.id)
			ids.AddID(id)
			idsAsBytes = ids.MustMarshal()

			if err := indexBucket.Put(indexedValue, idsAsBytes); err != nil {
				errChan <- err
				return err
			}

			refs.setIndexedValue(index.Name, index.SelectorHash, indexedValue)
		}
	}

	putErr := refsBucket.Put(refs.IDasBytes(), refs.asBytes())
	if putErr != nil {
		errChan <- err
		return err
	}

	return c.endOfIndexUpdate(ctx, tx, errChan, wgActions, wgCommitted)
	// })
}

func (c *Collection) onlyCleanRefs(ctx context.Context, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransaction) error {
	tx, txErr := c.db.Begin(true)
	if txErr != nil {
		errChan <- txErr
		return txErr
	}
	// return c.db.Update(func(tx *bolt.Tx) error {
	err := c.cleanRefs(ctx, tx, writeTransaction.id)
	if err != nil {
		errChan <- err
		return err
	}

	return c.endOfIndexUpdate(ctx, tx, errChan, wgActions, wgCommitted)
	// })
}

func (c *Collection) endOfIndexUpdate(ctx context.Context, tx *bolt.Tx, errChan chan error, wgActions, wgCommitted *sync.WaitGroup) error {
	// Tells the rest of the callers that the index is done but not committed
	wgActions.Done()

	// Wait for the store insetion to be completed
	err := waitForDoneErrOrCanceled(ctx, wgActions, nil)
	if err != nil {
		errChan <- err
		return err
	}

	fmt.Println("before commit index", tx.DB().Stats().TxN, tx.DB().Stats().OpenTxN)

	// // After commit is done successfully the function is called
	// tx.OnCommit(func() {
	// 	// Propagate the commit done status
	// 	wgCommitted.Done()
	// })
	err = tx.Commit()
	if err != nil {
		fmt.Println("error on commit")
		errChan <- err
		return err
	}

	wgCommitted.Done()
	fmt.Println("after commit succeed index")

	return nil
}

func (c *Collection) cleanRefs(ctx context.Context, tx *bolt.Tx, idAsString string) error {
	indexBucket := tx.Bucket([]byte("indexes"))
	refsBucket := tx.Bucket([]byte("refs"))

	// Get the references of the given ID
	refsAsBytes := refsBucket.Get(buildBytesID(idAsString))
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
				// If reference present in this index the reference is cleaned
				ids, newIDErr := newIDs(ctx, 0, nil, indexBucket.Bucket([]byte(index.Name)).Get(ref.IndexedValue))
				if newIDErr != nil {
					return newIDErr
				}
				ids.RmID(idAsString)
				// And saved again after the clean
				if err := indexBucket.Bucket([]byte(index.Name)).Put(ref.IndexedValue, ids.MustMarshal()); err != nil {
					return err
				}
			}
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

	// This count the number of running index query for this actual collection query
	nbToDo := 0

	// Goes through all index of the collection to define which index
	// will take care of the given filter
	for _, index := range c.indexes {
		for _, filter := range q.filters {
			if index.doesFilterApplyToIndex(filter) {
				go index.query(ctx, filter, finishedChan)
				nbToDo++
			}
		}
	}

	if nbToDo == 0 {
		return nil, fmt.Errorf("no index found")
	}

	// Loop every response from the index query
	for {
		select {
		case tmpIDs := <-finishedChan:
			if tmpIDs != nil {
				// Add IDs into the response tree
				for _, id := range tmpIDs.IDs {
					// Try to get the id from the tree
					fromTree := tree.Get(id)
					if fromTree == nil {
						// If not in the tree add it
						id.Increment()
						tree.ReplaceOrInsert(id)
						continue
					}
					// if already increment the counter
					fromTree.(*idType).Increment()
				}
			}
			// Save the fact that one more query has been respond
			nbToDo--
			// If nomore query to wait, quit the loop
			if nbToDo <= 0 {
				return tree, nil
			}
		case <-ctx.Done():
			time.Sleep(time.Millisecond * 100)
			return nil, ErrTimeOut
		}
	}
}

func (c *Collection) queryCleanAndOrder(ctx context.Context, q *Query, tree *btree.BTree) (response *ResponseQuery, _ error) {
	getRefFunc := func(id string) (refs *refs) {
		c.db.View(func(tx *bolt.Tx) error {
			refs, _ = c.getRefs(tx, id)
			return nil
		})
		return refs
	}

	// iterate the response tree to get only IDs which has been found in every index queries
	occurrenceFunc, idsSlice := occurrenceTreeIterator(len(q.filters), q.internalLimit, q.order, getRefFunc)
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
	response = newResponseQuery(len(idsMs.IDs))
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

		response.list[i] = &responseQueryElem{
			ID:             idsSlice.IDs[i],
			ContentAsBytes: responsesAsBytes[i],
		}
	}
	return
}

func (c *Collection) putIntoStore(ctx context.Context, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransaction) error {
	ret := c.store.Update(func(txn *badger.Txn) error {
		storeID := c.buildStoreID(writeTransaction.id)
		setErr := txn.Set(storeID, writeTransaction.contentAsBytes)
		if setErr != nil {
			err := fmt.Errorf("error inserting %q: %s", writeTransaction.id, setErr.Error())
			errChan <- err
			return err
		}

		// Tells the rest of the callers that the index is done but not committed
		wgActions.Done()

		// Wait for the store insetion to be completed
		err := waitForDoneErrOrCanceled(ctx, wgActions, nil)
		if err != nil {
			return err
		}

		// Start the commit of the indexes
		err = txn.Commit(nil)
		if err != nil {
			return err
		}

		// Propagate the commit done status
		wgCommitted.Done()

		return nil
	})
	return ret
}

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

			contentAsBytes, getValErr := item.Value()
			if getValErr != nil {
				return getValErr
			}
			ret[i] = contentAsBytes
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

func (c *Collection) loadIndex() error {
	indexes := c.getIndexesFromConfigBucket()
	for _, index := range indexes {
		index.conf = c.conf
		index.getTx = c.db.Begin
	}
	c.indexes = indexes

	return nil
}

func (c *Collection) deleteItemFromIndexes(ctx context.Context, id string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		refs, getRefsErr := c.getRefs(tx, id)
		if getRefsErr != nil {
			return getRefsErr
		}

		for _, ref := range refs.Refs {
			indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(ref.IndexName))
			ids, err := newIDs(ctx, 0, nil, indexBucket.Get(ref.IndexedValue))
			if err != nil {
				return err
			}

			ids.RmID(id)

			indexBucket.Put(ref.IndexedValue, ids.MustMarshal())
		}

		return nil
	})
}

func (c *Collection) getRefs(tx *bolt.Tx, id string) (*refs, error) {
	refsBucket := tx.Bucket([]byte("refs"))

	refsAsBytes := refsBucket.Get(buildBytesID(id))
	refs := newRefsFromDB(refsAsBytes)
	if refs == nil {
		return nil, fmt.Errorf("references mal formed: %s", string(refsAsBytes))
	}
	return refs, nil
}

// getStoredIDs returns all ids if it does not exceed the limit.
// This will not returned the ID used to set the value inside the collection
// It returns the id used to set the value inside the store
func (c *Collection) getStoredIDsAndValues(starter string, limit int, IDsOnly bool) ([]*responseQueryElem, error) {
	response := make([]*responseQueryElem, limit)

	err := c.store.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := []byte(c.id[:4] + "_")
		if starter == "" {
			iter.Seek(prefix)
		} else {
			iter.Seek(append(prefix, []byte(starter)...))
		}

		count := 0
		for ; iter.Valid(); iter.Next() {
			if !iter.ValidForPrefix(prefix) || count > limit-1 {
				response = response[:count]
				return nil
			}

			responseItem := new(responseQueryElem)

			item := iter.Item()

			if item.IsDeletedOrExpired() {
				continue
			}

			responseItem.ID = new(idType)
			responseItem.ID.ID = string(item.Key()[5:])

			if !IDsOnly {
				var err error
				responseItem.ContentAsBytes = make([]byte, 1000*1000*2)
				// responseItem.ContentAsBytes = make([]byte, 1000*1000*2)
				responseItem.ContentAsBytes, err = item.ValueCopy(responseItem.ContentAsBytes)
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
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *Collection) indexAllValues(i *indexType) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 0)

	lastID := ""

newLoop:
	savedElements, getErr := c.getStoredIDsAndValues(lastID, 10, false)
	if getErr != nil {
		return getErr
	}

	if len(savedElements) <= 1 {
		return nil
	}

	fmt.Println("saved elements", savedElements[0].ID, string(savedElements[0].ContentAsBytes))

	for _, savedElement := range savedElements {
		if savedElement.ID.ID == lastID {
			continue
		}

		var elem interface{}
		if jsonErr := json.Unmarshal(savedElement.ContentAsBytes, &elem); jsonErr != nil {
			return jsonErr
		}

		m := elem.(map[string]interface{})

		ctx2, cancel2 := context.WithTimeout(ctx, c.conf.TransactionTimeOut)
		defer cancel2()

		tr := newTransaction(savedElement.ID.ID)
		tr.ctx = ctx2

		tr.contentInterface = m

		fmt.Println("sending", tr.id)

		fakeWgAction := new(sync.WaitGroup)
		fakeWgCommitted := new(sync.WaitGroup)
		fakeWgAction.Add(1)
		fakeWgCommitted.Add(1)
		go c.putIntoIndexes(ctx, errChan, fakeWgAction, fakeWgCommitted, tr)

		fmt.Println("start waiting", tr.id)

		fmt.Println("1")

		err := waitForDoneErrOrCanceled(ctx2, fakeWgCommitted, errChan)
		if err != nil {
			return err
		}

		lastID = tr.id
	}

	goto newLoop
}
