package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/dgraph-io/badger"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transactions"
)

// Put add the given content to database with the given ID
func (c *Collection) Put(id string, content interface{}) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.options.TransactionTimeOut)
	defer cancel()

	// verify that closing as not been called
	if !c.isRunning() {
		return ErrClosedDB
	}

	dbKey := c.buildIDWhitPrefixData([]byte(id))

	// var err error
	// var contentAsBytes []byte

	// if tryBytes, ok := content.([]byte); ok {
	// 	contentAsBytes = tryBytes
	// } else {
	// 	contentAsBytes, err = json.Marshal(content)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	contentAsBytes, err := c.getInterfaceAsBytes(content)
	if err != nil {
		return err
	}

	tr := transactions.NewTransaction(ctx)
	trElem := transactions.NewTransactionElement(dbKey, contentAsBytes)
	tr.AddTransaction(trElem)

	// Run the insertion
	c.writeTransactionChan <- tr

	var wg sync.WaitGroup
	for _, i := range c.bleveIndexes {
		contentToIndex, apply := i.Selector.Apply(content)
		if apply {
			wg.Add(1)
			go func() {
				i, err = c.getBleveIndex(i.Name)
				if err != nil {
					fmt.Println("err indexing 1", err)
					return
				}

				err = i.index.Index(id, contentToIndex)
				if err != nil {
					fmt.Println("err indexing 2", err, contentToIndex)
				}
				wg.Done()
			}()
			// i.index.Index(id, contentToIndex)
		}
	}
	wg.Wait()

	// And wait for the end of the insertion
	return <-tr.ResponseChan
}

// PutMulti put the given elements in the DB with one single write transaction.
// This must have much better performances than with multiple *Collection.Put().
// The number of IDs and of content must be equal.
func (c *Collection) PutMulti(IDs []string, content []interface{}) error {
	// Check the length of the parameters
	if len(IDs) != len(content) {
		return ErrPutMultiWrongLen
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.options.TransactionTimeOut)
	defer cancel()

	// verify that closing as not been called
	if !c.isRunning() {
		return ErrClosedDB
	}

	tr := transactions.NewTransaction(ctx)
	tr.Transactions = make([]*transactions.WriteElement, len(IDs))

	for i := range IDs {
		contentAsBytes, err := c.getInterfaceAsBytes(content[i])
		if err != nil {
			return err
		}

		tr.Transactions[i] = transactions.NewTransactionElement(
			c.buildIDWhitPrefixData([]byte(IDs[i])),
			contentAsBytes,
		)
	}

	// Run the insertion
	c.writeTransactionChan <- tr
	// And wait for the end of the insertion
	return <-tr.ResponseChan
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

	decoder := json.NewDecoder(bytes.NewBuffer(contentAsBytes))

	uMarshalErr := decoder.Decode(pointer)
	if uMarshalErr != nil {
		return nil, uMarshalErr
	}

	return contentAsBytes, nil
}

// Delete removes the corresponding object if the given ID
func (c *Collection) Delete(id string) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.options.TransactionTimeOut)
	defer cancel()

	// verify that closing as not been called
	if !c.isRunning() {
		return ErrClosedDB
	}

	tr := transactions.NewTransaction(ctx)
	trElem := transactions.NewTransactionElement(c.buildIDWhitPrefixData([]byte(id)), nil)

	tr.AddTransaction(trElem)

	// Run the insertion
	c.writeTransactionChan <- tr
	// And wait for the end of the insertion
	return <-tr.ResponseChan
}

// DeleteBleveIndex remove the bleve index from the collection
func (c *Collection) DeleteBleveIndex(name string) error {
	var index *bleveIndex

	// Find the correct index from the list
	for _, activeIndex := range c.bleveIndexes {
		if activeIndex.Name == name {
			index = activeIndex
		}
	}

	if index == nil {
		return ErrNotFound
	}

	indexPrefix := c.buildIDWhitPrefixBleveIndex([]byte(name), nil)
	for {
		done, err := deleteLoop(c.store, indexPrefix)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	for i, activeIndex := range c.bleveIndexes {
		if activeIndex.Name == name {
			// Clean the collection list from the index pointer
			copy(c.bleveIndexes[i:], c.bleveIndexes[i+1:])
			c.bleveIndexes[len(c.bleveIndexes)-1] = nil
			c.bleveIndexes = c.bleveIndexes[:len(c.bleveIndexes)-1]
		}
	}

	return c.saveCollections()
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
		ret[i] = record.GetID()
	}
	return ret, nil
}

// GetValues returns a list of IDs and values as bytes for the given collection and starting
// at the given ID. The limit paramiter let caller ask for a portion of the collection.
func (c *Collection) GetValues(startID string, limit int) ([]*Response, error) {
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
				return ErrRollbackVersionNotFound
			} else if previousVersion == 0 {
				item := iterator.Item()

				var asEncryptedBytes []byte
				asEncryptedBytes, err = item.ValueCopy(asEncryptedBytes)
				if err != nil {
					return err
				}
				var asBytes []byte
				asBytes, err = cipher.Decrypt(c.options.privateCryptoKey, item.Key(), asEncryptedBytes)
				if err != nil {
					return err
				}

				// Build a custom decoder to use the number interface instead of float64
				decoder := json.NewDecoder(bytes.NewBuffer(asBytes))

				decoder.Decode(&contentAsInterface)

				timestamp = item.Version()
				return nil
			}
			previousVersion--
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return timestamp, c.Put(id, contentAsInterface)
}

// GetBleveIndexesName returns the names of every bleve indexes from the given collection.
func (c *Collection) GetBleveIndexesName() (ret []string) {
	for _, i := range c.bleveIndexes {
		ret = append(ret, i.Name)
	}
	return
}

// GetBleveIndex returns a bleve index based on the given name
func (c *Collection) GetBleveIndex(name string) (bleve.Index, error) {
	index, err := c.getBleveIndex(name)
	if err != nil {
		return nil, err
	}

	return index.index, nil
}

// SetBleveIndex defines a new bleve index into the collection.
func (c *Collection) SetBleveIndex(name string, bleveMapping mapping.IndexMapping, selector ...string) error {
	for _, i := range c.bleveIndexes {
		if i.Name == name {
			return ErrIndexNameAllreadyExists
		}
	}

	i := new(bleveIndex)
	i.Name = name
	i.Selector = selector

	i.IndexPrefix = c.buildIDWhitPrefixBleveIndex([]byte(name), nil)

	// Path of the configuration
	i.Path = c.buildIndexPath(name)

	i.KvConfig = c.buildKvConfig(i.Path, i.IndexPrefix)
	bleveIndex, err := bleve.NewUsing(i.Path, bleveMapping, upsidedown.Name, blevestore.Name, i.KvConfig)
	if err != nil {
		return err
	}

	i.index = bleveIndex

	i.IndexDirZip, err = indexZipper(i.Path)
	if err != nil {
		return err
	}

	c.bleveIndexes = append(c.bleveIndexes, i)

	err = c.saveCollections()
	if err != nil {
		return err
	}

	i.indexAllValues(c)

	return nil
}

// Search is used to query a bleve index.
// Give the index name the corresponding request.
// It returns a SearchResult pointer.
func (c *Collection) Search(indexName string, searchRequest *bleve.SearchRequest) (*SearchResult, error) {
	ret := new(SearchResult)

	index, err := c.getBleveIndex(indexName)
	if err != nil {
		return nil, err
	}

	fmt.Println("search")
	ret.BleveSearchResult, err = index.index.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	if ret.BleveSearchResult.Hits.Len() == 0 {
		return nil, ErrNotFound
	}

	ret.c = c

	return ret, nil
}

// Next takes a destination pointer as argument and try to get the next value from the request to fill-up the destination.
// You can call this function directly after the query and until the end when it returns an error.
func (s *SearchResult) Next(dest interface{}) (*search.DocumentMatch, error) {
	if s.BleveSearchResult.Hits.Len()-1 < int(s.position) {
		return nil, ErrSearchOver
	}

	doc := s.BleveSearchResult.Hits[s.position]

	_, err := s.c.Get(doc.ID, dest)
	if err != nil {
		return nil, err
	}

	s.position++

	return doc, nil
}
