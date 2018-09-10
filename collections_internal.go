package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
	"github.com/google/btree"
	"github.com/minio/highwayhash"
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

		return confBucket.Put([]byte("name"), []byte(name))
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

		indexes = append(indexes, index)

		indexesAsBytes, _ = json.Marshal(indexes)
		return confBucket.Put([]byte("indexesList"), indexesAsBytes)
	})
}

func (c *Collection) initWriteTransactionChan(ctx context.Context) {
	// Set a limit
	limit := c.options.PutBufferLimit
	// Build the queue with 2 times the limit to help writing on disc
	// in the same order as the operation are called
	c.writeTransactionChan = make(chan *writeTransaction, limit*2)
	// Start the infinite loop

	go c.waittingWriteLoop(ctx, limit)
}

func (c *Collection) waittingWriteLoop(ctx context.Context, limit int) {
	for {
		select {
		// A request came up
		case tr := <-c.writeTransactionChan:
			// Build a new write request
			newTr := newTransaction(tr.ctx)
			// Add the first request to the waiting list
			newTr.addTransaction(tr.transactions...)

			// Build the slice of chan the writer will respond
			waittingForResponseList := []chan error{}
			// Same the first response channel
			waittingForResponseList = append(waittingForResponseList, tr.responseChan)

			// Try to empty the queue if any
		tryToGetAnOtherRequest:
			select {
			// There is an other request in the queue
			case trBis := <-c.writeTransactionChan:
				// Save the request
				newTr.addTransaction(trBis.transactions...)
				// And save the response channel
				waittingForResponseList = append(waittingForResponseList, trBis.responseChan)

				// Check if the limit is not reach
				if len(newTr.transactions) < limit {
					// If not lets try to empty the queue a bit more
					goto tryToGetAnOtherRequest
				}
				// This release continue if there is no request in the queue
			default:
			}

			// Run the write operation
			go c.writeTransactions(newTr)

			// Get the response
			err := <-newTr.responseChan
			// And spread the response to all callers in parallel
			for _, waittingForResponse := range waittingForResponseList {
				go func(waittingForResponse chan error, err error) {
					waittingForResponse <- err
				}(waittingForResponse, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Collection) writeTransactions(tr *writeTransaction) {
	// Build a waiting groups
	// This group is to make internal functions wait the otherone
	wgActions := new(sync.WaitGroup)
	// This group defines the waitgroup to consider that all have been done correctly.
	wgCommitted := new(sync.WaitGroup)

	// Used to propagate the error for one or the other function
	errChan := make(chan error, 1)
	if len(tr.transactions) == 1 {
		c.writeOneTransaction(tr.ctx, errChan, wgActions, wgCommitted, tr.transactions[0])

		// Respond to the caller with the error if any
		tr.responseChan <- waitForDoneErrOrCanceled(tr.ctx, wgCommitted, errChan)
	} else {
		tr.responseChan <- c.writeMultipleTransaction(tr.ctx, errChan, wgActions, wgCommitted, tr)
	}
}

func (c *Collection) writeOneTransaction(ctx context.Context, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, wtElem *writeTransactionElement) {
	// Increment the tow waiting groups
	wgActions.Add(2)
	wgCommitted.Add(2)

	if wtElem.isInsertion {
		// Runs saving into the store
		go c.putIntoStore(ctx, nil, errChan, wgActions, wgCommitted, wtElem)

		// Starts the indexing process
		if !wtElem.bin {
			go c.putIntoIndexes(ctx, nil, errChan, wgActions, wgCommitted, wtElem)
		} else {
			go c.onlyCleanRefs(ctx, nil, errChan, wgActions, wgCommitted, wtElem)
		}
	} else {
		// Else is because it's a deletation
		go c.delFromStore(ctx, nil, errChan, wgActions, wgCommitted, wtElem)
		go c.onlyCleanRefs(ctx, nil, errChan, wgActions, wgCommitted, wtElem)
	}
}

func (c *Collection) writeMultipleTransaction(ctx context.Context, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, wt *writeTransaction) error {
	badgerTxn := c.store.NewTransaction(true)
	defer badgerTxn.Discard()

	boltTx, beginBoltTxErr := c.db.Begin(true)
	if beginBoltTxErr != nil {
		errChan <- beginBoltTxErr
		return beginBoltTxErr
	}

	// Because there is only one commit for all insertion we add manually 1
	wgCommitted.Add(1)

	// Loop for every insertion
	for _, wtElem := range wt.transactions {
		// Increment the tow waiting groups
		wgActions.Add(2)

		// Build a new wait group to prevent concurant writes which make Badger panic
		var wgLoop sync.WaitGroup
		wgLoop.Add(2)

		if wtElem.isInsertion {
			// Runs saving into the store
			go c.putIntoStore(ctx, badgerTxn, errChan, wgActions, &wgLoop, wtElem)

			// Starts the indexing process
			if !wtElem.bin {
				go c.putIntoIndexes(ctx, boltTx, errChan, wgActions, &wgLoop, wtElem)
			} else {
				go c.onlyCleanRefs(ctx, boltTx, errChan, wgActions, &wgLoop, wtElem)
			}
		} else {
			// Else is because it's a deletation
			go c.delFromStore(ctx, badgerTxn, errChan, wgActions, &wgLoop, wtElem)
			go c.onlyCleanRefs(ctx, boltTx, errChan, wgActions, &wgLoop, wtElem)
		}

		// Wait for this to be saved in suspend before commit
		wgLoop.Wait()
	}

	// Tells to the rest that commit can be run now
	wgCommitted.Done()

	// Wait for error if any
	err := waitForDoneErrOrCanceled(ctx, wgCommitted, errChan)
	if err == nil {
		// Commit every thing if no error reported
		err = badgerTxn.Commit(nil)
		if err != nil {
			errChan <- err
			return err
		}
		err = boltTx.Commit()
		if err != nil {
			errChan <- err
			return err
		}
	}
	// Respond to the caller with the error if any
	return nil
}

func (c *Collection) buildStoreID(id string) []byte {
	return []byte(fmt.Sprintf("%s_%s", c.id[:4], id))
}

func (c *Collection) putIntoIndexes(ctx context.Context, tx *bolt.Tx, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransactionElement) error {
	multi := true
	if tx == nil {
		multi = false
		var txErr error
		tx, txErr = c.db.Begin(true)
		if txErr != nil {
			errChan <- txErr
			return txErr
		}
	}

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
		if indexedValues, apply := index.apply(writeTransaction.contentInterface); apply {
			indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(index.Name))

			// If the selector hit a slice.
			// apply can returns more than one value to index
			for _, indexedValue := range indexedValues {
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
	}

	putErr := refsBucket.Put(refs.IDasBytes(), refs.asBytes())
	if putErr != nil {
		errChan <- err
		return err
	}

	return c.endOfIndexUpdate(ctx, tx, !multi, errChan, wgActions, wgCommitted)
}

func (c *Collection) onlyCleanRefs(ctx context.Context, tx *bolt.Tx, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransactionElement) error {
	multi := true
	if tx == nil {
		multi = false
		var txErr error
		tx, txErr = c.db.Begin(true)
		if txErr != nil {
			errChan <- txErr
			return txErr
		}
	} else if !tx.Writable() {
		errChan <- bolt.ErrTxNotWritable
		return bolt.ErrTxNotWritable
	}

	err := c.cleanRefs(ctx, tx, writeTransaction.id)
	if err != nil {
		errChan <- err
		return err
	}

	return c.endOfIndexUpdate(ctx, tx, !multi, errChan, wgActions, wgCommitted)
}

func (c *Collection) endOfIndexUpdate(ctx context.Context, tx *bolt.Tx, commit bool, errChan chan error, wgActions, wgCommitted *sync.WaitGroup) error {
	// Tells the rest of the callers that the index is done but not committed
	wgActions.Done()

	// Wait for the store insetion to be completed
	err := waitForDoneErrOrCanceled(ctx, wgActions, errChan)
	if err != nil {
		errChan <- err
		return err
	}

	if commit {
		err = tx.Commit()
		if err != nil {
			errChan <- err
			tx.Rollback()
			return err
		}
	}

	wgCommitted.Done()

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
		c.db.View(func(tx *bolt.Tx) error {
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

func (c *Collection) insertOrDeleteStore(ctx context.Context, txn *badger.Txn, isInsertion bool, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransactionElement) error {
	multi := true
	if txn == nil {
		multi = false
		txn = c.store.NewTransaction(true)
		defer txn.Discard()
	}

	hashSignature, _ := uintToBytes((highwayhash.Sum64(writeTransaction.contentAsBytes, make([]byte, highwayhash.Size))))
	contentToWrite := append(hashSignature, writeTransaction.contentAsBytes...)

	storeID := c.buildStoreID(writeTransaction.id)

	var writeErr error
	if isInsertion {
		writeErr = txn.Set(storeID, contentToWrite)
	} else {
		writeErr = txn.Delete(storeID)
	}
	if writeErr != nil {
		err := fmt.Errorf("error writing %q: %s", writeTransaction.id, writeErr.Error())
		errChan <- err
		return err
	}

	// Tells the rest of the callers that the index is done but not committed
	wgActions.Done()

	// Wait for the store insetion to be completed
	err := waitForDoneErrOrCanceled(ctx, wgActions, errChan)
	if err != nil {
		return err
	}

	if !multi {
		// Start the commit of the indexes
		err = txn.Commit(nil)
		if err != nil {
			errChan <- err
			return err
		}
	}

	// Propagate the commit done status
	wgCommitted.Done()

	return nil
}
func (c *Collection) putIntoStore(ctx context.Context, txn *badger.Txn, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransactionElement) error {
	return c.insertOrDeleteStore(ctx, txn, true, errChan, wgActions, wgCommitted, writeTransaction)
}

func (c *Collection) delFromStore(ctx context.Context, txn *badger.Txn, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, writeTransaction *writeTransactionElement) error {
	return c.insertOrDeleteStore(ctx, txn, false, errChan, wgActions, wgCommitted, writeTransaction)
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

func (c *Collection) loadIndex() error {
	indexes := c.getIndexesFromConfigBucket()
	for _, index := range indexes {
		index.options = c.options
		index.getTx = c.db.Begin
	}
	c.indexes = indexes

	return nil
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
func (c *Collection) getStoredIDsAndValues(starter string, limit int, IDsOnly bool) ([]*ResponseElem, error) {
	response := make([]*ResponseElem, limit)

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

			responseItem := new(ResponseElem)

			item := iter.Item()

			if item.IsDeletedOrExpired() {
				continue
			}

			responseItem._ID = new(idType)
			responseItem._ID.ID = string(item.Key()[5:])

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

	errChan := make(chan error, 0)

	lastID := ""

newLoop:
	savedElements, getErr := c.getStoredIDsAndValues(lastID, 100, false)
	if getErr != nil {
		return getErr
	}

	if len(savedElements) <= 1 {
		return nil
	}

	tx, txErr := c.db.Begin(true)
	if txErr != nil {
		errChan <- txErr
		return txErr
	}

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

		fakeWgAction := new(sync.WaitGroup)
		fakeWgCommitted := new(sync.WaitGroup)
		fakeWgAction.Add(1)
		fakeWgCommitted.Add(1)

		trElement := newTransactionElement(savedElement.GetID(), m, true)

		go c.putIntoIndexes(ctx2, tx, errChan, fakeWgAction, fakeWgCommitted, trElement)

		err := waitForDoneErrOrCanceled(ctx2, fakeWgCommitted, errChan)
		if err != nil {
			return err
		}

		lastID = savedElement.GetID()
	}

	err := tx.Commit()
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
