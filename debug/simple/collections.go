package simple

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/debug/simple/transaction"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

type (
	Collection struct {
		*dbElement

		db *DB

		BleveIndexes []*BleveIndex
	}
)

func NewCollection(name string) *Collection {
	return &Collection{
		dbElement: &dbElement{
			Name: name,
		},
	}
}

func (c *Collection) SetBleveIndex(name string, bleveMapping mapping.IndexMapping, selector ...string) (err error) {
	index := NewIndex(name)
	index.Name = name
	indexHash := blake2b.Sum256([]byte(name))
	index.Prefix = append(c.Prefix, indexHash[:2]...)
	index.Selector = selector
	index.Path = fmt.Sprintf("%s/%x/%x", c.db.Path, c.Prefix, index.Prefix)

	config := blevestore.NewBleveStoreConfigMap(index.Path, c.db.PrivateKey, c.Prefix, c.db.Badger, c.db.writeChan)
	index.BleveIndex, err = bleve.NewUsing(index.Path, bleve.NewIndexMapping(), upsidedown.Name, blevestore.Name, config)
	if err != nil {
		return
	}

	c.BleveIndexes = append(c.BleveIndexes, index)

	return
}

func (c *Collection) Put(id string, content interface{}) (err error) {
	var tr *transaction.Transaction
	if bytes, ok := content.([]byte); ok {
		tr = transaction.NewTransaction(c.buildDBKey(id), bytes, false)
	} else {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return marshalErr
		}

		tr = transaction.NewTransaction(c.buildDBKey(id), jsonBytes, false)
	}

	c.db.writeChan <- tr
	err = <-tr.ResponseChan
	if err != nil {
		return
	}

	for _, index := range c.BleveIndexes {
		contentToIndex, apply := index.Selector.Apply(content)
		if apply {
			err = index.BleveIndex.Index(id, contentToIndex)
			if err != nil {
				return err
			}
		}
	}

	return
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
	tr := transaction.NewTransaction(c.buildDBKey(id), nil, true)
	fmt.Println("need to rm INDEX")

	c.db.writeChan <- tr
	return <-tr.ResponseChan
}

func (c *Collection) buildDBKey(id string) []byte {
	return append(c.Prefix, []byte(id)...)
}
