package simple

import (
	"encoding/json"
	"fmt"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/debug/simple/transaction"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/blevesearch/bleve/mapping"
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
	index.Prefix = append(c.Prefix, indexHash[:]...)
	index.Selector = selector
	fmt.Println(c.db.Path)
	fmt.Println(c.Prefix)
	fmt.Println(index.Prefix)
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
		tr = transaction.NewTransaction(append(c.Prefix, []byte(id)...), bytes, false)
	} else {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return marshalErr
		}

		tr = transaction.NewTransaction(append(c.Prefix[:], []byte(id)...), jsonBytes, false)
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

func (c *Collection) Delete(id string) (err error) {
	tr := transaction.NewTransaction(append(c.Prefix[:], []byte(id)...), nil, true)
	fmt.Println("need to rm INDEX")

	c.db.writeChan <- tr
	return <-tr.ResponseChan
}
