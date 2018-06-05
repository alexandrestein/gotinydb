package gotinydb

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

type (
	// Collection defines the storage object
	Collection struct {
		Name, ID string
		Indexes  []*Index

		DB    *bolt.DB
		Store *badger.DB
	}
)

// Put add the given content to database with the given ID
func (c *Collection) Put(id string, content interface{}) error {
	isBin := false
	contentAsBytes := []byte{}
	if bytes, ok := content.([]byte); ok {
		isBin = true
		contentAsBytes = bytes
	}

	if !isBin {
		jsonBytes, marshalErr := json.Marshal(content)
		if marshalErr != nil {
			return marshalErr
		}

		contentAsBytes = jsonBytes
	}

	if err := c.put(id, contentAsBytes); err != nil {
		return err
	}

	if !isBin {
		if err := c.putIntoIndexes(id, content); err != nil {
			return err
		}
	}

	return nil
}
func (c *Collection) put(id string, content []byte) error {
	if mainInsertErr := c.Store.Update(func(txn *badger.Txn) error {
		return txn.Set(c.buildStoreID(id), content)
	}); mainInsertErr != nil {
		return mainInsertErr
	}
	return nil
}

// Get retreives the content of the given ID
func (c *Collection) Get(id string, pointer interface{}) error {
	if id == "" {
		return vars.ErrEmptyID
	}
	idAsBytes := c.buildStoreID(id)

	contentAsBytes := []byte{}

	if err := c.Store.View(func(txn *badger.Txn) error {
		item, getError := txn.Get(idAsBytes)
		if getError != nil {
			return getError
		}

		if item.IsDeletedOrExpired() {
			return vars.ErrNotFound
		}

		var getValErr error
		contentAsBytes, getValErr = item.Value()
		if getValErr != nil {
			return getValErr
		}
		return nil
	}); err != nil {
		return err
	}

	if givenBuffer, ok := pointer.(*bytes.Buffer); ok {
		if len(contentAsBytes) != 0 {
			givenBuffer.Write(contentAsBytes)
			return nil
		}
		return fmt.Errorf("content of %q is empty or not present", id)
	}

	uMarshalErr := json.Unmarshal(contentAsBytes, pointer)
	if uMarshalErr != nil {
		return uMarshalErr
	}

	return nil
}

// Delete removes the corresponding object if the given ID
func (c *Collection) Delete(id string) error {
	if id == "" {
		return vars.ErrEmptyID
	}

	return c.Store.Update(func(txn *badger.Txn) error {
		return txn.Delete(c.buildStoreID(id))
	})
}
