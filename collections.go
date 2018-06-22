package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
	"github.com/google/btree"
)

type (
	// Collection defines the storage object
	Collection struct {
		Name, ID string
		Indexes  []*Index

		DB    *bolt.DB
		Store *badger.DB

		writeTransactionChan chan *writeTransaction

		transactionTimeout time.Duration

		ctx context.Context
	}
)

// Put add the given content to database with the given ID
func (c *Collection) Put(id string, content interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.transactionTimeout)
	defer cancel()

	tr := newTransaction(id)
	tr.ctx = ctx
	tr.contentInterface = content

	if bytes, ok := content.([]byte); ok {
		tr.bin = true
		tr.contentAsBytes = bytes
	}

	if !tr.bin {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return marshalErr
		}

		tr.contentAsBytes = jsonBytes
	}

	// Run the insertion
	c.writeTransactionChan <- tr
	// And wait for the end of the insertion
	err := <-tr.responseChan
	if err != nil {
		return err
	}

	return nil
}

func (c *Collection) putIntoStore(ctx context.Context, storeErrChan, indexErrChan chan error, writeTransaction *writeTransaction) error {
	return c.Store.Update(func(txn *badger.Txn) error {
		err := txn.Set(c.buildStoreID(writeTransaction.id), writeTransaction.contentAsBytes)
		if err != nil {
			err = fmt.Errorf("error inserting %q: %s", writeTransaction.id, err.Error())
			storeErrChan <- err
			return err
		}

		close(storeErrChan)

		nbTry := 0
	waitForEnd:
		// Wait for the index process to end.
		// If any error the actions are rollbacked
		select {
		case err, ok := <-indexErrChan:
			if !ok {
				return nil
			}
			if err != nil {
				return fmt.Errorf("issue on the index: %s", err.Error())
			}
		case <-ctx.Done():
			if writeTransaction.done {
				if nbTry < 5 {
					goto waitForEnd
				}
				nbTry++
			}
			return ctx.Err()
		}
		return nil
	})
}

// Get retrieves the content of the given ID
func (c *Collection) Get(id string, pointer interface{}) ([]byte, error) {
	if id == "" {
		return nil, vars.ErrEmptyID
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.transactionTimeout)
	defer cancel()

	contentAsBytes := []byte{}

	response, getErr := c.get(ctx, id)
	if getErr != nil {
		return nil, getErr
	}
	contentAsBytes = response[0]

	if len(contentAsBytes) == 0 {
		return nil, fmt.Errorf("content of %q is empty or not present", id)
	}

	if pointer == nil {
		return contentAsBytes, nil
	}

	uMarshalErr := json.Unmarshal(contentAsBytes, pointer)
	if uMarshalErr != nil {
		return nil, uMarshalErr
	}

	return contentAsBytes, nil
}

func (c *Collection) get(ctx context.Context, ids ...string) ([][]byte, error) {
	ret := make([][]byte, len(ids))
	if err := c.Store.View(func(txn *badger.Txn) error {
		for i, id := range ids {
			idAsBytes := c.buildStoreID(id)
			item, getError := txn.Get(idAsBytes)
			if getError != nil {
				if getError == badger.ErrKeyNotFound {
					return vars.ErrNotFound
				}
				return getError
			}

			if item.IsDeletedOrExpired() {
				return vars.ErrNotFound
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

// Delete removes the corresponding object if the given ID
func (c *Collection) Delete(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.transactionTimeout)
	defer cancel()

	if id == "" {
		return vars.ErrEmptyID
	}

	if rmStoreErr := c.Store.Update(func(txn *badger.Txn) error {
		return txn.Delete(c.buildStoreID(id))
	}); rmStoreErr != nil {
		return rmStoreErr
	}

	return c.deleteIndexes(ctx, id)
}

// SetIndex enable the collection to index field or sub field
func (c *Collection) SetIndex(i *Index) error {
	c.Indexes = append(c.Indexes, i)
	if err := c.DB.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.Bucket([]byte("indexes")).CreateBucket([]byte(i.Name))
		if createErr != nil {
			return createErr
		}
		return nil
	}); err != nil {
		return err
	}

	i.getIDsFunc = func(ctx context.Context, indexedValue []byte) (ids *IDs, err error) {
		if err := c.DB.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
			asBytes := bucket.Get(indexedValue)
			ids, err = NewIDs(asBytes)
			return err
		}); err != nil {
			return nil, err
		}
		return ids, nil
	}

	i.getRangeIDsFunc = func(ctx context.Context, indexedValue []byte, keepEqual, increasing bool) (allIDs *IDs, err error) {
		if err := c.DB.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
			// Initiate the cursor (iterator)
			iter := bucket.Cursor()
			// Go to the requested position
			firstIndexedValueAsByte, firstIDsAsByte := iter.Seek(indexedValue)
			firstIDsValue, unmarshalIDsErr := NewIDs(firstIDsAsByte)
			if unmarshalIDsErr != nil {
				return unmarshalIDsErr
			}

			allIDs, _ = NewIDs(nil)

			// if the asked value is found
			if reflect.DeepEqual(firstIndexedValueAsByte, indexedValue) && keepEqual {
				allIDs.AddIDs(firstIDsValue)
			}

			var nextFunc func() (key []byte, value []byte)
			if increasing {
				nextFunc = iter.Next
			} else {
				nextFunc = iter.Prev
			}

			for {
				indexedValue, idsAsByte := nextFunc()
				if len(indexedValue) <= 0 && len(idsAsByte) <= 0 {
					break
				}
				ids, unmarshalIDsErr := NewIDs(idsAsByte)
				if unmarshalIDsErr != nil {
					return unmarshalIDsErr
				}
				allIDs.AddIDs(ids)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return allIDs, nil
	}

	// i.setIDFunc = func(ctx context.Context, storeErr, indexErr chan error, indexedValue []byte, idAsString string) {
	// 	if err := c.DB.Update(func(tx *bolt.Tx) error {
	// 		indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
	// 		refsBucket := tx.Bucket([]byte("refs"))

	// 		idsAsBytes := indexBucket.Get(indexedValue)
	// 		ids, parseIDsErr := NewIDs(idsAsBytes)
	// 		if parseIDsErr != nil {
	// 			return parseIDsErr
	// 		}

	// 		id := NewID(idAsString)
	// 		ids.AddID(id)
	// 		idsAsBytes = ids.MustMarshal()

	// 		if err := indexBucket.Put(indexedValue, idsAsBytes); err != nil {
	// 			return err
	// 		}

	// 		refsAsBytes := refsBucket.Get(vars.BuildBytesID(id.String()))
	// 		refs := NewRefs()
	// 		if refsAsBytes == nil && len(refsAsBytes) > 0 {
	// 			if err := json.Unmarshal(refsAsBytes, refs); err != nil {
	// 				return err
	// 			}
	// 		}

	// 		refs.ObjectID = id.String()
	// 		refs.ObjectHashID = vars.BuildID(id.String())
	// 		refs.SetIndexedValue(i.Name, indexedValue)

	// 		putErr := refsBucket.Put(refs.IDasBytes(), refs.AsBytes())
	// 		if putErr != nil {
	// 			return nil
	// 		}

	// 		indexErr <- nil

	// 		select {
	// 		case _, ok := <-storeErr:
	// 			if !ok {
	// 				return nil
	// 			}
	// 			return fmt.Errorf("issue on the store")
	// 		case <-ctx.Done():
	// 			return ctx.Err()
	// 		}

	// 	}); err != nil {
	// 		indexErr <- err
	// 		return
	// 	}
	// }

	return nil
}

// Query run the given query to all the collection indexes
func (c *Collection) Query(q *Query) (response *ResponseQuery, _ error) {
	if q == nil {
		return
	}

	// If no filter the query stops
	if len(q.filters) <= 0 {
		return nil, fmt.Errorf("query has not get action")
	}

	// If no index stop the query
	if len(c.Indexes) <= 0 {
		return
	}

	// Set a timout
	ctx, cancel := context.WithTimeout(context.Background(), c.transactionTimeout)
	defer cancel()

	// Init the destination
	tree := btree.New(10)

	// Initialize the channel which will confirm that all queries are done
	finishedChan := make(chan *IDs, 16)
	defer close(finishedChan)

	// This count the number of running index query for this actual collection query
	nbToDo := 0

	// Goes through all index of the collection to define which index
	// will take care of the given filter
	for _, index := range c.Indexes {
		for _, filter := range q.filters {
			if index.DoesFilterApplyToIndex(filter) {
				go index.Query(ctx, filter, finishedChan)
				nbToDo++
			}
		}
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
						id.Increment(ctx)
						tree.ReplaceOrInsert(id)
						continue
					}
					// if allready increment the counter
					fromTree.(*ID).Increment(ctx)
				}
			}
			// Save the fact that one more query has been respond
			nbToDo--
			// If nomore query to wait, quit the loop
			if nbToDo <= 0 {
				goto queriesDone
			}
		case <-ctx.Done():
			return nil, vars.ErrTimeOut
		}

	}

queriesDone:

	// iterate the response tree to get only IDs which has been found in every index queries
	fn, ret := iterator(len(q.filters), q.limit)
	tree.Ascend(fn)

	// Build the response for the caller
	response = NewResponseQuery(len(ret.IDs))
	response.query = q
	// Get every content of the query from the database
	responsesAsBytes, err := c.get(ctx, ret.Strings()...)
	if err != nil {
		return nil, err
	}

	// Range the response values as slice of bytes
	for i := range responsesAsBytes {
		if i >= q.limit {
			break
		}

		response.List[i] = &ResponseQueryElem{
			ID:             ret.IDs[i],
			ContentAsBytes: responsesAsBytes[i],
		}
	}

	return
}

func (c *Collection) deleteIndexes(ctx context.Context, id string) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		refsBucket := tx.Bucket([]byte("refs"))

		refsAsBytes := refsBucket.Get(vars.BuildBytesID(id))
		refs := NewRefsFromDB(refsAsBytes)
		if refs == nil {
			return fmt.Errorf("references mal formed: %s", string(refsAsBytes))
		}

		for _, ref := range refs.Refs {
			indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(ref.IndexName))
			ids, err := NewIDs(indexBucket.Get(ref.IndexedValue))
			if err != nil {
				return err
			}

			ids.RmID(id)

			indexBucket.Put(ref.IndexedValue, ids.MustMarshal())
		}

		return nil
	})
}
