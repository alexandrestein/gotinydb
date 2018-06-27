package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"

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

		Conf *Conf

		DB    *bolt.DB
		Store *badger.DB

		writeTransactionChan chan *writeTransaction

		ctx context.Context
	}
)

// Put add the given content to database with the given ID
func (c *Collection) Put(id string, content interface{}) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.Conf.TransactionTimeOut)
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
	return <-tr.responseChan
}

func (c *Collection) putIntoStore(ctx context.Context, errChan chan error, doneChan chan bool, writeTransaction *writeTransaction) error {
	defer func() { doneChan <- true }()
	ret := c.Store.Update(func(txn *badger.Txn) error {
		setErr := txn.Set(c.buildStoreID(writeTransaction.id), writeTransaction.contentAsBytes)
		if setErr != nil {
			err := fmt.Errorf("error inserting %q: %s", writeTransaction.id, setErr.Error())
			errChan <- err
			return err
		}

		errChan <- nil

		select {
		case ok := <-doneChan:
			if ok {
				txn.Commit(nil)
				return nil
			}
			return fmt.Errorf("error from outsid of the store")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	return ret
}

// Get retrieves the content of the given ID
func (c *Collection) Get(id string, pointer interface{}) ([]byte, error) {
	if id == "" {
		return nil, vars.ErrEmptyID
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.Conf.TransactionTimeOut)
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
	ctx, cancel := context.WithTimeout(context.Background(), c.Conf.TransactionTimeOut)
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
	i.conf = c.Conf
	i.getTx = c.DB.Begin

	c.Indexes = append(c.Indexes, i)
	if err := c.setIndexesIntoConfigBucket(i); err != nil {
		return err
	}

	if err := c.DB.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.Bucket([]byte("indexes")).CreateBucket([]byte(i.Name))
		if createErr != nil {
			return createErr
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (c *Collection) loadIndex() error {
	indexes := c.getIndexesFromConfigBucket()
	for _, index := range indexes {
		index.conf = c.Conf
		index.getTx = c.DB.Begin
	}
	c.Indexes = indexes

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
		return nil, fmt.Errorf("no index in the collection")
	}

	if q.internalLimit > c.Conf.InternalQueryLimit {
		q.internalLimit = c.Conf.InternalQueryLimit
	}
	if q.timeout > c.Conf.QueryTimeOut {
		q.timeout = c.Conf.QueryTimeOut
	}

	// Set a timout
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
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
					// if allready increment the counter
					fromTree.(*ID).Increment()
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

	getRefFunc := func(id string) (refs *Refs) {
		c.DB.View(func(tx *bolt.Tx) error {
			refs, _ = c.getRefs(tx, id)
			return nil
		})
		return refs
	}

	// iterate the response tree to get only IDs which has been found in every index queries
	occurrenceFunc, retTree := occurrenceTreeIterator(len(q.filters), q.internalLimit, q.order, getRefFunc)
	tree.Ascend(occurrenceFunc)

	// get the ids in the order and with the given limit
	orderFunc, ret := orderTreeIterator(q.limit)
	if q.ascendent {
		retTree.Ascend(orderFunc)
	} else {
		retTree.Descend(orderFunc)
	}

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
		refs, getRefsErr := c.getRefs(tx, id)
		if getRefsErr != nil {
			return getRefsErr
		}

		for _, ref := range refs.Refs {
			indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(ref.IndexName))
			ids, err := NewIDs(ctx, 0, nil, indexBucket.Get(ref.IndexedValue))
			if err != nil {
				return err
			}

			ids.RmID(id)

			indexBucket.Put(ref.IndexedValue, ids.MustMarshal())
		}

		return nil
	})
}

func (c *Collection) getRefs(tx *bolt.Tx, id string) (*Refs, error) {
	refsBucket := tx.Bucket([]byte("refs"))

	refsAsBytes := refsBucket.Get(vars.BuildBytesID(id))
	refs := NewRefsFromDB(refsAsBytes)
	if refs == nil {
		return nil, fmt.Errorf("references mal formed: %s", string(refsAsBytes))
	}
	return refs, nil
}

// GetAllStoreIDs returns all ids if it does not exceed the limit.
// This will not returned the ID used to set the value inside the collection
// It returns the id used to set the value inside the store
func (c *Collection) getAllStoreIDs(limit int) ([][]byte, error) {
	ids := make([][]byte, limit)

	err := c.Store.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := []byte(c.ID[:4] + "_")
		iter.Seek(prefix)

		count := 0
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			if !iter.ValidForPrefix(prefix) || count >= limit-1 {
				ids = ids[:count]
				return nil
			}

			ids[count] = item.Key()

			count++
		}

		ids = ids[:count]
		return nil
	})
	if err != nil {
		return nil, err
	}

	return ids, nil
}
