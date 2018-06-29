package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

// Put add the given content to database with the given ID
func (c *Collection) Put(id string, content interface{}) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.conf.TransactionTimeOut)
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
	s := <-tr.responseChan
	return s
}

// Get retrieves the content of the given ID
func (c *Collection) Get(id string, pointer interface{}) (contentAsBytes []byte, _ error) {
	if id == "" {
		return nil, ErrEmptyID
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.conf.TransactionTimeOut)
	defer cancel()

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

// Delete removes the corresponding object if the given ID
func (c *Collection) Delete(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.conf.TransactionTimeOut)
	defer cancel()

	if id == "" {
		return ErrEmptyID
	}

	if rmStoreErr := c.store.Update(func(txn *badger.Txn) error {
		return txn.Delete(c.buildStoreID(id))
	}); rmStoreErr != nil {
		return rmStoreErr
	}

	return c.deleteIndexes(ctx, id)
}

// SetIndex enable the collection to index field or sub field
func (c *Collection) SetIndex(name string, t IndexType, selector ...string) error {
	i := newIndex(name, t, selector...)
	i.conf = c.conf
	i.getTx = c.db.Begin

	c.indexes = append(c.indexes, i)
	if errSetingIndexIntoConfig := c.setIndexesIntoConfigBucket(i); errSetingIndexIntoConfig != nil {
		return errSetingIndexIntoConfig
	}

	if updateErr := c.db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.Bucket([]byte("indexes")).CreateBucket([]byte(i.Name))
		if createErr != nil {
			return createErr
		}
		return nil
	}); updateErr != nil {
		return updateErr
	}
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
	if len(c.indexes) <= 0 {
		return nil, fmt.Errorf("no index in the collection")
	}

	if q.internalLimit > c.conf.InternalQueryLimit {
		q.internalLimit = c.conf.InternalQueryLimit
	}
	if q.timeout > c.conf.QueryTimeOut {
		q.timeout = c.conf.QueryTimeOut
	}

	// Set a timout
	ctx, cancel := context.WithTimeout(context.Background(), q.timeout)
	defer cancel()

	tree, err := c.queryGetIDs(ctx, q)
	if err != nil {
		return nil, err
	}

	return c.queryCleanAndOrder(ctx, q, tree)
}
