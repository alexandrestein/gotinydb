package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

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
			if getError == badger.ErrKeyNotFound {
				return vars.ErrNotFound
			}
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

// SetIndex enable the collection to index field or sub field
func (c *Collection) SetIndex(i *Index) error {
	c.Indexes = append(c.Indexes, i)
	if err := c.DB.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucket([]byte(i.Name))
		if createErr != nil {
			return createErr
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// Query run the given query to all the collection indexes
func (c *Collection) Query(q *Query) (ids []string) {
	if q == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	getIDsChan := make(chan []string, 16)
	getIDs := []string{}
	cleanIDsChan := make(chan []string, 16)
	cleanIDs := []string{}

	for _, index := range c.Indexes {
		go index.RunQuery(ctx, q.GetActions, getIDsChan)
		go index.RunQuery(ctx, q.CleanActions, cleanIDsChan)
	}

	getDone, cleanDone := false, false

	for {
		select {
		case retIDs, ok := <-getIDsChan:
			if ok {
				getIDs = append(getIDs, retIDs...)
			} else {
				getDone = true
			}

			if getDone && cleanDone {
				goto afterFilters
			}
		case retIDs, ok := <-cleanIDsChan:
			if ok {
				cleanIDs = append(cleanIDs, retIDs...)
			} else {
				cleanDone = true
			}

			if getDone && cleanDone {
				goto afterFilters
			}
		case <-ctx.Done():
			return
		}
	}

afterFilters:
	ids = getIDs

	// Clean the retreived IDs of the clean selection
	for j := len(ids) - 1; j >= 0; j-- {
		for _, cleanID := range cleanIDs {
			if len(ids) <= j {
				continue
			}
			if ids[j] == cleanID {
				ids = append(ids[:j], ids[j+1:]...)
				continue
			}
		}
		if q.Distinct {
			keys := make(map[string]bool)
			list := []string{}
			if _, value := keys[ids[j]]; !value {
				keys[ids[j]] = true
				list = append(list, ids[j])
			}
			ids = list
		}
	}

	// Do the limit
	if len(ids) > q.Limit {
		ids = ids[:q.Limit]
	}

	// Reverts the result if wanted
	if q.InvertedOrder {
		for i := len(ids)/2 - 1; i >= 0; i-- {
			opp := len(ids) - 1 - i
			ids[i], ids[opp] = ids[opp], ids[i]
		}
	}

	return ids
}
