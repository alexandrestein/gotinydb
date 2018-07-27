package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

// Put add the given content to database with the given ID
func (c *Collection) Put(id string, content interface{}) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.options.TransactionTimeOut)
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

	ctx, cancel := context.WithTimeout(context.Background(), c.options.TransactionTimeOut)
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
	ctx, cancel := context.WithTimeout(context.Background(), c.options.TransactionTimeOut)
	defer cancel()

	if id == "" {
		return ErrEmptyID
	}

	if rmStoreErr := c.store.Update(func(txn *badger.Txn) error {
		return txn.Delete(c.buildStoreID(id))
	}); rmStoreErr != nil {
		return rmStoreErr
	}

	return c.deleteItemFromIndexes(ctx, id)
}

// SetIndex enable the collection to index field or sub field
func (c *Collection) SetIndex(name string, t IndexType, selector ...string) error {
	i := newIndex(name, t, selector...)
	i.options = c.options
	i.getTx = c.db.Begin

	if updateErr := c.db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.Bucket([]byte("indexes")).CreateBucket([]byte(i.Name))
		if createErr != nil {
			return createErr
		}
		return nil
	}); updateErr != nil {
		return updateErr
	}

	if err := c.indexAllValues(i); err != nil {
		return err
	}

	c.indexes = append(c.indexes, i)
	if errSetingIndexIntoConfig := c.setIndexesIntoConfigBucket(i); errSetingIndexIntoConfig != nil {
		return errSetingIndexIntoConfig
	}

	return nil
}

// DeleteIndex remove the index from the collection
func (c *Collection) DeleteIndex(name string) error {
	// Find the correct index from the list
	for i, activeIndex := range c.indexes {
		if activeIndex.Name == name {
			// Clean the collection list from the index pointer
			copy(c.indexes[i:], c.indexes[i+1:])
			c.indexes[len(c.indexes)-1] = nil
			c.indexes = c.indexes[:len(c.indexes)-1]

			// Remove the all index from indexes database
			return c.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("indexes")).DeleteBucket([]byte(name))
			})
		}
	}

	return ErrNotFound
}

// Query run the given query to all the collection indexes
func (c *Collection) Query(q *Query) (response *Response, _ error) {
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

	if q.internalLimit > c.options.InternalQueryLimit {
		q.internalLimit = c.options.InternalQueryLimit
	}
	if q.timeout > c.options.QueryTimeOut {
		q.timeout = c.options.QueryTimeOut
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

// GetIDs returns a list of IDs for the given collection and starting
// at the given ID. The limit paramiter let caller ask for a portion of the collection.
func (c *Collection) GetIDs(startID string, limit int) ([]string, error) {
	records, getElemErr := c.getStoredIDsAndValues(startID, limit, true)
	if getElemErr != nil {
		return nil, getElemErr
	}

	ret := make([]string, len(records))
	for i, record := range records {
		ret[i] = record.ID.ID
	}
	return ret, nil
}

// GetValues returns a list of IDs and values as bytes for the given collection and starting
// at the given ID. The limit paramiter let caller ask for a portion of the collection.
func (c *Collection) GetValues(startID string, limit int) ([]*ResponseElem, error) {
	return c.getStoredIDsAndValues(startID, limit, false)
}

// Rollback reset content to a previous version for the given key.
// The database by default keeps 10 version of the same key.
// previousVersion provide a way to get the wanted version where 0 is the fist previous
// content and bigger previousVersion is older the content will be.
// It returns the previous asked version timestamp.
// Everytime this function is called a new version is added.
func (c *Collection) Rollback(id string, previousVersion uint) (timestamp uint64, err error) {
	var contentAsInterface interface{}
	found := false

	err = c.store.View(func(txn *badger.Txn) error {
		// Init the iterator
		iterator := txn.NewIterator(
			badger.IteratorOptions{
				AllVersions:    true,
				PrefetchSize:   c.options.BadgerOptions.NumVersionsToKeep,
				PrefetchValues: true,
			},
		)
		defer iterator.Close()

		// Set the rollback to at least the immediate previous content
		previousVersion = previousVersion + 1

		// Seek to the wanted key
		// Loop to the version
		for iterator.Seek(c.buildStoreID(id)); iterator.Valid(); iterator.Next() {
			if !reflect.DeepEqual(c.buildStoreID(id), iterator.Item().Key()) {
				return fmt.Errorf("passed to an other key before hitting the requested version")
			} else if previousVersion == 0 {
				item := iterator.Item()
				asBytes, valueErr := item.Value()
				if valueErr != nil {
					return valueErr
				}

				unmarshalErr := json.Unmarshal(asBytes[8:], &contentAsInterface)
				if unmarshalErr != nil {
					return unmarshalErr
				}

				timestamp = item.Version()
				found = true
				return nil
			}
			previousVersion--
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	if !found {
		return 0, fmt.Errorf("the prior version %d was not found", previousVersion)
	}

	putErr := c.Put(id, contentAsInterface)
	if putErr != nil {
		return 0, putErr
	}

	return timestamp, nil
}
