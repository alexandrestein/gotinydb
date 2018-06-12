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

	if rmStoreErr := c.Store.Update(func(txn *badger.Txn) error {
		return txn.Delete(c.buildStoreID(id))
	}); rmStoreErr != nil {
		return rmStoreErr
	}

	return c.deleteIndexes(id)

	// for _, index := range c.Indexes {
	// 	err := index.rmIDFunc(id)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// return nil
}

// SetIndex enable the collection to index field or sub field
func (c *Collection) SetIndex(i *Index) error {
	c.Indexes = append(c.Indexes, i)
	if err := c.DB.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.Bucket([]byte("indexes")).CreateBucket([]byte(i.Name))
		if createErr != nil {
			return createErr
		}
		return nil
	}); err != nil {
		return err
	}

	i.getIDsFunc = func(indexedValue []byte) (ids *IDs, err error) {
		if err := c.DB.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
			ids, err = NewIDs(bucket.Get(indexedValue))
			return err
		}); err != nil {
			return nil, err
		}
		return ids, nil
	}

	i.getRangeIDsFunc = func(indexedValue []byte, keepEqual, increasing bool, nb int) (allIDs *IDs, err error) {
		if err := c.DB.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
			// Initiate the cursor (iterator)
			iter := bucket.Cursor()
			// Go to the requested position
			firstIndexedValueAsByte, firstIDsAsByte := iter.Seek(indexedValue)
			firstIDsValue, unmarshalIDsErr := NewIDs(firstIDsAsByte)
			if unmarshalIDsErr != nil {
				return unmarshalIDsErr
			}

			allIDs, _ = NewIDs(nil)

			// if the asked value is found
			if reflect.DeepEqual(firstIndexedValueAsByte, indexedValue) && keepEqual {
				allIDs.AddIDs(firstIDsValue)
			}

			var nextFunc func() (key []byte, value []byte)
			if increasing {
				nextFunc = iter.Next
			} else {
				nextFunc = iter.Prev
			}

			for {
				if len(allIDs.IDs) >= nb {
					allIDs.IDs = allIDs.IDs[:nb]
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
				allIDs.AddIDs(ids)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return allIDs, nil
	}

	i.setIDFunc = func(indexedValue []byte, idAsString string) error {
		return c.DB.Update(func(tx *bolt.Tx) error {
			indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
			refsBucket := tx.Bucket([]byte("refs"))

			idsAsBytes := indexBucket.Get(indexedValue)
			ids, parseIDsErr := NewIDs(idsAsBytes)
			if parseIDsErr != nil && len(idsAsBytes) != 0 {
				return parseIDsErr
			}

			id := ID(idAsString)
			ids.AddID(&id)
			var formatErr error
			idsAsBytes, formatErr = ids.Marshal()
			if formatErr != nil {
				return formatErr
			}

			if err := indexBucket.Put(indexedValue, idsAsBytes); err != nil {
				return err
			}

			refsAsBytes := refsBucket.Get(vars.BuildBytesID(id.String()))
			refs := NewRefs()
			if refsAsBytes == nil && len(refsAsBytes) > 0 {
				if err := json.Unmarshal(refsAsBytes, refs); err != nil {
					return err
				}
			}

			refs.ObjectID = id.String()
			refs.ObjectHashID = vars.BuildID(id.String())
			refs.SetIndexedValue(i.Name, indexedValue)

			return refsBucket.Put(refs.IDasBytes(), refs.AsBytes())
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

	finishedChan := make(chan *IDs, 16)
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
			if tmpIDs != nil {
				for _, id := range tmpIDs.IDs {
					tree.ReplaceOrInsert(id)
				}
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
			for _, id := range tmpIDs.IDs {
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

	for _, id := range ret.IDs {
		ids = append(ids, id.String())
	}

	return
}

func (c *Collection) deleteIndexes(id string) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		refsBucket := tx.Bucket([]byte("refs"))

		refsAsBytes := refsBucket.Get(vars.BuildBytesID(id))
		refs := NewRefsFromDB(refsAsBytes)
		if refs == nil {
			return fmt.Errorf("references mal formed: %s", string(refsAsBytes))
		}

		for _, ref := range refs.Refs {
			indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(ref.IndexName))
			ids, err := NewIDs(indexBucket.Get(ref.IndexedValue))
			if err != nil {
				return err
			}

			ids.RmID(id)
		}

		return nil
	})
}
