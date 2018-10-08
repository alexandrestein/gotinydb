package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

type (
	Collection struct {
		dbElement

		db *DB

		BleveIndexes []*BleveIndex
	}
)

func NewCollection(name string) *Collection {
	return &Collection{
		dbElement: dbElement{
			Name: name,
		},
	}
}

func (c *Collection) buildIndexPrefix() []byte {
	prefix := make([]byte, len(c.Prefix))
	copy(prefix, c.Prefix)
	prefix = append(prefix, prefixCollectionsBleveIndex)
	return prefix
}

func (c *Collection) SetBleveIndex(name string, bleveMapping mapping.IndexMapping) (err error) {
	prefix := c.buildIndexPrefix()
	indexHash := blake2b.Sum256([]byte(name))
	prefix = append(prefix, indexHash[:2]...)

	for _, i := range c.BleveIndexes {
		if i.Name == name {
			return ErrIndexNameAllreadyExists
		}
		if reflect.DeepEqual(i.Prefix, prefix) {
			return ErrHashCollision
		}
	}

	index := NewIndex(name)
	index.Name = name
	index.Prefix = prefix

	colHash := blake2b.Sum256([]byte(c.Name))
	index.Path = fmt.Sprintf("%s/%x/%x", c.db.Path, colHash[:2], indexHash[:2])

	config := blevestore.NewBleveStoreConfigMap(c.db.ctx, index.Path, c.db.PrivateKey, prefix, c.db.Badger, c.db.writeChan)
	index.BleveIndex, err = bleve.NewUsing(index.Path, bleveMapping, upsidedown.Name, blevestore.Name, config)
	if err != nil {
		return
	}

	index.BleveIndexAsBytes, err = index.indexZipper()
	if err != nil {
		return err
	}

	c.BleveIndexes = append(c.BleveIndexes, index)

	// Index all existing values
	err = c.db.Badger.View(func(txn *badger.Txn) error {
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
			clearBytes, err = cipher.Decrypt(c.db.PrivateKey, item.Key(), itemAsEncryptedBytes)

			id := string(item.Key()[len(colPrefix):])

			content := c.fromValueBytesGetContentToIndex(clearBytes)
			err = index.BleveIndex.Index(id, content)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return c.db.saveConfig()
}

func (c *Collection) Put(id string, content interface{}) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tr *transaction.Transaction
	if bytes, ok := content.([]byte); ok {
		tr = transaction.NewTransaction(ctx, c.buildDBKey(id), bytes, false)
	} else {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return marshalErr
		}

		tr = transaction.NewTransaction(ctx, c.buildDBKey(id), jsonBytes, false)
	}

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
	if err != nil {
		return
	}

	for _, index := range c.BleveIndexes {
		err = index.BleveIndex.Index(id, content)
		if err != nil {
			return err
		}
	}

	return
}

func (c *Collection) fromValueBytesGetContentToIndex(input []byte) interface{} {
	var elem interface{}
	decoder := json.NewDecoder(bytes.NewBuffer(input))

	if jsonErr := decoder.Decode(&elem); jsonErr != nil {
		fmt.Println("errjsonErr", jsonErr)
		return nil
	}

	var ret interface{}
	typed := elem.(map[string]interface{})
	ret = typed

	return ret
}

func (c *Collection) Get(id string, pointer interface{}) (contentAsBytes []byte, err error) {
	if id == "" {
		return nil, ErrEmptyID
	}

	bdKey := c.buildDBKey(id)

	c.db.Badger.View(func(txn *badger.Txn) (err error) {
		var item *badger.Item
		item, err = txn.Get(bdKey)
		if err != nil {
			return err
		}
		contentAsBytes, err = item.ValueCopy(contentAsBytes)
		if err != nil {
			return err
		}

		return nil
	})

	contentAsBytes, err = c.db.decryptData(bdKey, contentAsBytes)
	if err != nil {
		return nil, err
	}

	if pointer == nil {
		return contentAsBytes, nil
	}

	decoder := json.NewDecoder(bytes.NewBuffer(contentAsBytes))
	decoder.UseNumber()

	uMarshalErr := decoder.Decode(pointer)
	if uMarshalErr != nil {
		return nil, uMarshalErr
	}

	return contentAsBytes, nil
}

func (c *Collection) Delete(id string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := transaction.NewTransaction(ctx, c.buildDBKey(id), nil, true)

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
		err = index.BleveIndex.Delete(id)
		if err != nil {
			return err
		}
	}

	return
}

func (c *Collection) buildDBKey(id string) []byte {
	key := append(c.Prefix, prefixCollectionsData)
	return append(key, []byte(id)...)
}

func (c *Collection) GetBleveIndex(name string) (*BleveIndex, error) {
	for _, bi := range c.BleveIndexes {
		if bi.Name == name {
			return bi, nil
		}
	}
	return nil, ErrIndexNotFound
}

func (c *Collection) Search(indexName string, searchRequest *bleve.SearchRequest) (*SearchResult, error) {
	ret := new(SearchResult)

	index, err := c.GetBleveIndex(indexName)
	// bleveIndex, err := c.GetBleveIndex(indexName)
	if err != nil {
		return nil, err
	}

	ret.BleveSearchResult, err = index.BleveIndex.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	if ret.BleveSearchResult.Hits.Len() == 0 {
		return nil, ErrNotFound
	}

	ret.c = c

	return ret, nil
}
