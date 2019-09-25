package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

type (
	// Collection defines the base element for saving objects. It holds the indexes and the values.
	Collection struct {
		dbElement

		db *DB
		// BleveIndexes in public for marshalling reason and should never be used directly
		BleveIndexes []*BleveIndex
	}

	// Batch is a simple struct to manage multiple write in one commit
	Batch struct {
		c  *Collection
		tr *transaction.Transaction
	}

	multiGetCaller struct {
		id                        string
		dbID                      []byte
		i                         int
		pointer                   interface{}
		asBytes, encryptedAsBytes []byte
		err                       error
	}
)

func newCollection(name string) *Collection {
	return &Collection{
		dbElement: dbElement{
			name: name,
		},
	}
}

// buildIndexPrefix builds the prefix for indexes.
// It copies the buffer to prevent mutations.
func (c *Collection) buildIndexPrefix() []byte {
	prefix := make([]byte, len(c.prefix))
	copy(prefix, c.prefix)
	prefix = append(prefix, prefixCollectionsBleveIndex)
	return prefix
}

// GetName returns the collection name
func (c *Collection) GetName() string {
	return c.name
}

// SetBleveIndex adds a bleve index to the collection.
// It build a new index with the given index mapping.
func (c *Collection) SetBleveIndex(name string, documentMapping *mapping.DocumentMapping) (err error) {
	// Use only the tow first bytes as index prefix.
	// The prefix is used to confine indexes with a prefixes.
	prefix := c.buildIndexPrefix()
	indexHash := blake2b.Sum256([]byte(name))
	prefix = append(prefix, indexHash[:2]...)

	// ok, start building a new index
	index := newIndex(name)
	index.name = name
	index.collection = c
	index.prefix = prefix
	err = index.buildSignature(documentMapping)
	if err != nil {
		return err
	}

	// Check there is no conflict name or hash
	for _, i := range c.BleveIndexes {
		if i.name == name {
			if !bytes.Equal(i.Signature[:], index.Signature[:]) {
				return ErrIndexAllreadyExistsWithDifferentMapping
			}
			return ErrNameAllreadyExists
		}
		if reflect.DeepEqual(i.prefix, prefix) {
			return ErrHashCollision
		}
	}

	// Bleve needs to save some parts on the drive.
	// The path is based on a part of the collection hash and the index prefix.
	colHash := blake2b.Sum256([]byte(c.name))
	index.Path = fmt.Sprintf("%x%s%x", colHash[:2], string(os.PathSeparator), indexHash[:2])

	// Build the index and set the given document index as default
	bleveMapping := bleve.NewIndexMapping()
	bleveMapping.StoreDynamic = false
	bleveMapping.IndexDynamic = true
	bleveMapping.DocValuesDynamic = false

	for _, fieldMapping := range documentMapping.Fields {
		fieldMapping.Store = false
		fieldMapping.Index = true
	}
	bleveMapping.DefaultMapping = documentMapping

	// Build the configuration to use the local bleve storage and initialize the index
	config := blevestore.NewConfigMap(c.db.ctx, index.Path, c.db.privateKey, prefix, c.db.badger, c.db.writeChan)
	index.bleveIndex, err = bleve.NewUsing(c.db.path+string(os.PathSeparator)+index.Path, bleveMapping, upsidedown.Name, blevestore.Name, config)
	if err != nil {
		return
	}

	// Save the on drive bleve element into the index struct itself
	index.BleveIndexAsBytes, err = index.indexZipper()
	if err != nil {
		return err
	}

	// Add the new index to the list of index of this collection
	c.BleveIndexes = append(c.BleveIndexes, index)

	// Index all existing values
	err = c.db.badger.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		colPrefix := c.buildDBKey("")
		for iter.Seek(colPrefix); iter.ValidForPrefix(colPrefix); iter.Next() {
			item := iter.Item()

			var err error
			var itemAsEncryptedBytes []byte
			itemAsEncryptedBytes, err = item.ValueCopy(itemAsEncryptedBytes)
			if err != nil {
				continue
			}

			var clearBytes []byte
			clearBytes, err = cipher.Decrypt(c.db.privateKey, item.Key(), itemAsEncryptedBytes)

			id := string(item.Key()[len(colPrefix):])

			content := c.fromValueBytesGetContentToIndex(clearBytes)
			err = index.bleveIndex.Index(id, content)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Save the new settup
	return c.db.saveConfig()
}

func (c *Collection) putSendToWriteAndWaitForResponse(tr *transaction.Transaction) (err error) {
	select {
	case c.db.writeChan <- tr:
	case <-c.db.ctx.Done():
		return c.db.ctx.Err()
	}

	select {
	case err = <-tr.ResponseChan:
	case <-tr.Ctx.Done():
		err = tr.Ctx.Err()
	}

	return err
}

func (c *Collection) putLoopForIndexes(tr *transaction.Transaction) (err error) {
	for _, index := range c.BleveIndexes {
		for _, op := range tr.Operations {
			// If remove the content no need to index it
			if op.Delete || op.Content == nil && op.CollectionID == "" {
				continue
			}

			err = index.bleveIndex.Index(op.CollectionID, op.Content)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Collection) put(id string, content interface{}, clean bool, ttl time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr, err := c.NewBatch(ctx)
	if err != nil {
		return err
	}

	if clean {
		err = tr.PutClean(id, content)
	} else if ttl > 0 {
		err = tr.PutWithTTL(id, content, ttl)
	} else {
		err = tr.Put(id, content)
	}
	if err != nil {
		return err
	}

	return c.writeBatch(tr)
}

// PutWithCleanHistory set the content to the given id but clean all previous records of this id
func (c *Collection) PutWithCleanHistory(id string, content interface{}) (err error) {
	return c.put(id, content, true, 0)
}

// Put sets a new element into the collection.
// If the content match some of the indexes it will be indexed
func (c *Collection) Put(id string, content interface{}) error {
	return c.put(id, content, false, 0)
}

// PutWithTTL does the same as *Collection.Put but removes the content and
// its ID after the given duration
func (c *Collection) PutWithTTL(id string, content interface{}, ttl time.Duration) error {
	return c.put(id, content, false, ttl)
}

// NewBatch build a new write transaction to do all write operation in one commit
func (c *Collection) NewBatch(ctx context.Context) (*Batch, error) {
	tr := transaction.New(ctx)

	ret := new(Batch)
	ret.c = c
	ret.tr = tr

	return ret, nil
}

// BuildOperation builds a new operation to add in a transaction
func (c *Collection) buildOperation(id string, content interface{}, delete, cleanHistory bool) (*transaction.Operation, error) {
	var bytes []byte
	if tmpBytes, ok := content.([]byte); ok {
		bytes = tmpBytes
	} else {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return nil, marshalErr
		}
		bytes = jsonBytes
	}

	return transaction.NewOperation(id, content, c.buildDBKey(id), bytes, delete, cleanHistory), nil
}

// writeBatch gives a simple access to batch operations
func (c *Collection) writeBatch(b *Batch) (err error) {
	err = c.putSendToWriteAndWaitForResponse(b.tr)
	if err != nil {
		return err
	}

	return c.putLoopForIndexes(b.tr)
}

func (c *Collection) fromValueBytesGetContentToIndex(input []byte) interface{} {
	var elem interface{}
	decoder := json.NewDecoder(bytes.NewBuffer(input))

	if jsonErr := decoder.Decode(&elem); jsonErr != nil {
		return nil
	}

	var ret interface{}
	typed := elem.(map[string]interface{})
	ret = typed

	return ret
}

func (c *Collection) getEncrypted(txn *badger.Txn, caller *multiGetCaller) (err error) {
	if caller.id == "" {
		return ErrEmptyID
	}

	c.buildDBKey(caller.id)

	var item *badger.Item
	item, err = txn.Get(caller.dbID)
	if err != nil {
		return err
	}
	caller.encryptedAsBytes, err = item.ValueCopy(caller.encryptedAsBytes)
	if err != nil {
		return err
	}

	return
}

func (c *Collection) buildGetCaller(txn *badger.Txn, id string, dest interface{}) (caller *multiGetCaller, err error) {
	if id == "" {
		return nil, ErrEmptyID
	}

	caller = new(multiGetCaller)
	caller.id = id
	caller.pointer = dest
	caller.dbID = c.buildDBKey(id)

	return
}

func (c *Collection) get(txn *badger.Txn, id string, dest interface{}) (contentAsBytes []byte, err error) {
	var caller *multiGetCaller
	caller, err = c.buildGetCaller(txn, id, dest)
	if err != nil {
		return nil, err
	}

	err = c.getEncrypted(txn, caller)
	if err != nil {
		return nil, err
	}

	err = c.decryptAndUnmarshal(caller)
	if err != nil {
		return nil, err
	}

	dest = caller.pointer

	return caller.asBytes, nil
}

func (c *Collection) decryptAndUnmarshal(caller *multiGetCaller) (err error) {
	var contentAsBytes []byte
	contentAsBytes, err = c.db.decryptData(caller.dbID, caller.encryptedAsBytes)
	if err != nil {
		return err
	}

	caller.asBytes = contentAsBytes

	if caller.pointer == nil {
		return nil
	}

	decoder := json.NewDecoder(bytes.NewBuffer(contentAsBytes))
	decoder.UseNumber()

	uMarshalErr := decoder.Decode(caller.pointer)
	if uMarshalErr != nil {
		return uMarshalErr
	}

	return nil
}

// Get returns the saved element. It fills up the given dest pointer if provided.
// It always returns the content as a stream of bytes and an error if any.
func (c *Collection) Get(id string, dest interface{}) (contentAsBytes []byte, err error) {
	c.db.badger.View(func(txn *badger.Txn) error {
		contentAsBytes, err = c.get(txn, id, dest)
		return nil
	})

	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	}

	return
}

// GetMulti open one badger transaction and get all document concurrently
func (c *Collection) GetMulti(ids []string, destinations []interface{}) (contentsAsBytes [][]byte, err error) {
	if len(ids) != len(destinations) {
		return nil, ErrGetMultiNotEqual
	}

	contentsAsBytes = make([][]byte, len(ids))

	var wg sync.WaitGroup
	wg.Add(len(ids))

	respChan := make(chan *multiGetCaller, len(ids))

	c.db.badger.View(func(txn *badger.Txn) error {
		for i, id := range ids {
			var caller *multiGetCaller
			caller, err = c.buildGetCaller(txn, id, destinations[i])
			if err != nil {
				caller.err = err
				return err
			}

			err = c.getEncrypted(txn, caller)
			if err != nil {
				caller.err = err
				return err
			}

			go func() {
				c.decryptAndUnmarshal(caller)
				if caller.err != nil {
					caller.err = err
				}
				respChan <- caller
			}()
		}
		return nil
	})

	go func() {
		for {
			caller := <-respChan
			if caller.err != nil {
				err = caller.err
				return
			}

			contentsAsBytes[caller.i] = caller.asBytes

			wg.Done()
		}
	}()

	if err != nil {
		return nil, err
	}

	wg.Wait()

	return
}

// Delete deletes all references of the given id.
func (c *Collection) Delete(id string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := transaction.New(ctx)
	tr.AddOperation(
		transaction.NewOperation(id, nil, c.buildDBKey(id), nil, true, false),
	)

	// Send to the write channel
	select {
	case c.db.writeChan <- tr:
	case <-c.db.ctx.Done():
		return c.db.ctx.Err()
	}

	// Wait for response from the write routine
	select {
	case err = <-tr.ResponseChan:
	case <-tr.Ctx.Done():
		err = tr.Ctx.Err()
	}

	// Deletes from index
	for _, index := range c.BleveIndexes {
		err = index.bleveIndex.Delete(id)
		if err != nil {
			return err
		}
	}

	c.deleteRelatedFiles(id)

	return
}

// deleteRelatedFiles removes every files which is related to the given document
func (c *Collection) deleteRelatedFiles(id string) {
	fileIDs := c.db.fileStore.getRelatedFileIDs(c.name, id)
	for _, fileID := range fileIDs {
		c.db.fileStore.DeleteFile(fileID)
	}
	c.db.fileStore.deleteRelatedFileIDs(c.name, id, fileIDs...)
}

func (c *Collection) buildDBKey(id string) []byte {
	key := append(c.prefix, prefixCollectionsData)
	return append(key, []byte(id)...)
}

// buildToJustBigDBPrefix this is used when iterating values from the last one.
// It gives the smalles to big prefix for the collection.
func (c *Collection) buildJustTooBigDBPrefix() []byte {
	return append(c.prefix, prefixCollectionsData+1)
}

// GetBleveIndex gives an  easy way to interact directly with bleve
func (c *Collection) GetBleveIndex(name string) (*BleveIndex, error) {
	for _, bi := range c.BleveIndexes {
		if bi.name == name {
			return bi, nil
		}
	}
	return nil, ErrIndexNotFound
}

// Search make a search with the default bleve search request bleve.NewSearchRequest()
// and returns a local SearchResult pointer
func (c *Collection) Search(indexName string, query query.Query) (*SearchResult, error) {
	searchRequest := bleve.NewSearchRequest(query)

	return c.SearchWithOptions(indexName, searchRequest)
}

// SearchWithOptions does the same as *Collection.Search but you provide the searchRequest
func (c *Collection) SearchWithOptions(indexName string, searchRequest *bleve.SearchRequest) (*SearchResult, error) {
	ret := new(SearchResult)

	index, err := c.GetBleveIndex(indexName)
	if err != nil {
		return nil, err
	}

	ret.BleveSearchResult, err = index.bleveIndex.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	if ret.BleveSearchResult.Hits.Len() == 0 {
		return nil, ErrNotFound
	}

	ret.c = c

	return ret, nil
}

// History returns the previous versions of the given id.
// The first value is the actual value and more you travel inside the list more the
// records are old.
func (c *Collection) History(id string, limit int) (valuesAsBytes [][]byte, err error) {
	valuesAsBytes = make([][]byte, limit)
	count := 0

	return valuesAsBytes, c.db.badger.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.AllVersions = true
		iter := txn.NewIterator(opt)
		defer iter.Close()

		dbKey := c.buildDBKey(id)
		breakAtNext := false
		for iter.Seek(dbKey); iter.ValidForPrefix(dbKey); iter.Next() {
			if count >= limit || breakAtNext {
				break
			}

			item := iter.Item()
			if item.DiscardEarlierVersions() {
				breakAtNext = true
			}

			var content []byte
			content, err = item.ValueCopy(content)
			if err != nil {
				return err
			}

			content, err = c.db.decryptData(item.Key(), content)
			if err != nil {
				return err
			}

			valuesAsBytes[count] = content

			count++
		}

		// Clean the end of the slice to gives only the existing values
		valuesAsBytes = valuesAsBytes[:count]

		return nil
	})
}

// DeleteIndex delete the index and all references
func (c *Collection) DeleteIndex(name string) {
	var index *BleveIndex
	for i, tmpIndex := range c.BleveIndexes {
		if tmpIndex.name == name {
			index = tmpIndex

			copy(c.BleveIndexes[i:], c.BleveIndexes[i+1:])
			c.BleveIndexes[len(c.BleveIndexes)-1] = nil // or the zero value of T
			c.BleveIndexes = c.BleveIndexes[:len(c.BleveIndexes)-1]

			break
		}
	}

	index.close()
	index.delete()

	c.db.deletePrefix(index.prefix)
}

func (c *Collection) getIterator(reverted bool) *CollectionIterator {
	iterOptions := badger.DefaultIteratorOptions
	iterOptions.Reverse = reverted

	txn := c.db.badger.NewTransaction(false)
	badgerIter := txn.NewIterator(iterOptions)

	tmpPrefix := c.buildDBKey("")
	prefix := make([]byte, len(tmpPrefix))
	copy(prefix, tmpPrefix)

	baseIterator := &baseIterator{
		txn:        txn,
		badgerIter: badgerIter,
	}

	return &CollectionIterator{
		baseIterator: baseIterator,
		c:            c,
		colPrefix:    prefix,
	}
}

// GetIterator provides an easy way to list elements
func (c *Collection) GetIterator() *CollectionIterator {
	iter := c.getIterator(false)
	iter.badgerIter.Seek(iter.colPrefix)
	return iter
}

// GetRevertedIterator does same as above but work in the opposite way
func (c *Collection) GetRevertedIterator() *CollectionIterator {
	iter := c.getIterator(true)
	iter.badgerIter.Seek(c.buildJustTooBigDBPrefix())
	return iter
}

// addOperation add an operation to the existing Transaction pointer
func (b *Batch) addOperation(id string, content interface{}, delete, cleanHistory bool) error {
	op, err := b.c.buildOperation(id, content, delete, cleanHistory)
	if err != nil {
		return err
	}

	b.tr.AddOperation(op)

	return nil
}

// Put add a put operation to the existing Transaction pointer
func (b *Batch) Put(id string, content interface{}) error {
	return b.addOperation(id, content, false, false)
}

// PutWithTTL add a put operation to the existing Transaction pointer.
// The ttl parameter tels the database who long to keep the record in DB.
// After the given duration the content and the ID are removed from DB with clean history.
func (b *Batch) PutWithTTL(id string, content interface{}, ttl time.Duration) error {
	err := b.addOperation(id, content, false, false)
	if err != nil {
		return err
	}

	ttlObj := newTTL(b.c.name, id, false, ttl)
	op := transaction.NewOperation("", nil, ttlObj.timeAsKey(), ttlObj.exportAsBytes(), false, false)

	b.tr.AddOperation(op)
	return nil
}

// PutClean add a put operation to the existing Transaction pointer but clean
// existing history of the id
func (b *Batch) PutClean(id string, content interface{}) error {
	return b.addOperation(id, content, false, true)
}

// Delete add a delete operation to the existing Transaction pointer
func (b *Batch) Delete(id string) error {
	return b.addOperation(id, nil, true, true)
}

// Write execute the batch
func (b *Batch) Write() error {
	return b.c.writeBatch(b)
}
