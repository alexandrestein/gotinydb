package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

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

	i.getIDsFunc = func(indexedValue []byte) (ids []*ID, err error) {
		if err := c.DB.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(i.Name))
			ids, err = NewIDs(bucket.Get(indexedValue))
			return err
		}); err != nil {
			return nil, err
		}
		return ids, nil
	}

	i.getRangeIDsFunc = func(indexedValue []byte, keepEqual, increasing bool, nb int) (allIDs []*ID, err error) {
		if err := c.DB.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(i.Name))
			// Initiate the cursor (iterator)
			iter := bucket.Cursor()
			// Go to the requested position
			firstIndexedValueAsByte, firstIDsAsByte := iter.Seek(indexedValue)
			firstIDsValue, unmarshalIDsErr := NewIDs(firstIDsAsByte)
			if unmarshalIDsErr != nil {
				return unmarshalIDsErr
			}

			// if the asked value is found
			if reflect.DeepEqual(firstIndexedValueAsByte, indexedValue) && keepEqual {
				allIDs = append(allIDs, firstIDsValue...)
			}

			var nextFunc func() (key []byte, value []byte)
			if increasing {
				nextFunc = iter.Next
			} else {
				nextFunc = iter.Prev
			}

			for {
				if len(allIDs) >= nb {
					allIDs = allIDs[:nb]
					return nil
				}

				indexedValue, idsAsByte := nextFunc()
				if len(indexedValue) <= 0 && len(idsAsByte) <= 0 {
					break
				}
				ids, unmarshalIDsErr := NewIDs(idsAsByte)
				if unmarshalIDsErr != nil {
					return unmarshalIDsErr
				}
				allIDs = append(allIDs, ids...)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return allIDs, nil
	}

	i.addIDFunc = func(indexedValue []byte, id string) error {
		return c.DB.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(i.Name))

			idsAsBytes := bucket.Get(indexedValue)
			ids, parseIDsErr := vars.ParseIDsBytesToIDsAsStrings(idsAsBytes)
			if parseIDsErr != nil && len(idsAsBytes) != 0 {
				return parseIDsErr
			}

			ids = append(ids, id)
			var formatErr error
			idsAsBytes, formatErr = vars.FormatIDsStringsToIDsAsBytes(ids)
			if formatErr != nil {
				return formatErr
			}

			return bucket.Put(indexedValue, idsAsBytes)
		})
	}

	i.rmIDFunc = func(indexedValue []byte, idToRemove string) error {
		return c.DB.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(i.Name))

			// Get the saved IDs of the given field value
			idsAsBytes := bucket.Get(indexedValue)
			// Convert the slice of byte to slice of strings
			ids, parseIDsErr := vars.ParseIDsBytesToIDsAsStrings(idsAsBytes)
			if parseIDsErr != nil {
				return parseIDsErr
			}

			// Save the slot where the ID is found
			slotsToClean := []int{}
			for j, id := range ids {
				if id == idToRemove {
					slotsToClean = append(slotsToClean, j)
				}
			}

			// Remove the pointed values from the above loop
			for j := len(slotsToClean) - 1; j > 0; j-- {
				n := slotsToClean[j]
				ids = append(ids[:n], ids[n+1:]...)
			}

			// Format and save the new list of IDs for the given indexed value
			var formatErr error
			idsAsBytes, formatErr = vars.FormatIDsStringsToIDsAsBytes(ids)
			if formatErr != nil {
				return formatErr
			}

			return bucket.Put(indexedValue, idsAsBytes)
		})
	}

	return nil
}

// Query run the given query to all the collection indexes
func (c *Collection) Query(q *Query) (ids []string, _ error) {
	if q == nil {
		return
	}

	if len(q.getActions) <= 0 {
		return nil, fmt.Errorf("query has not get action")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	tree := btree.New(10)

	finishedChan := make(chan []*ID, 16)
	defer close(finishedChan)
	nbToDo := 0

	// If no index stop the query
	if len(c.Indexes) <= 0 {
		return
	}

	for _, index := range c.Indexes {
		for _, action := range q.getActions {
			go index.Query(ctx, action, finishedChan)
			nbToDo++
		}
	}

	for {
		select {
		case tmpIDs := <-finishedChan:
			for _, id := range tmpIDs {
				tree.ReplaceOrInsert(id)
			}
			nbToDo--
			if nbToDo <= 0 {
				goto getDone
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("get context timeout")
		}

	}

getDone:
	if len(q.cleanActions) <= 0 {
		goto cleanDone
	}

	nbToDo = 0
	for _, index := range c.Indexes {
		for _, action := range q.cleanActions {
			go index.Query(ctx, action, finishedChan)
			nbToDo++
		}
	}

	for {
		select {
		case tmpIDs := <-finishedChan:
			for _, id := range tmpIDs {
				tree.Delete(id)
			}
			nbToDo--
			if nbToDo <= 0 {
				goto cleanDone
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("clean context timeout")
		}
	}

cleanDone:

	fn, ret := iterator(q.limit)
	tree.Ascend(fn)

	for _, id := range ret.Slice {
		ids = append(ids, id.String())
	}

	return
}
