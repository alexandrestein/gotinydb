package gotinydb

import (
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

		nbTransaction          int
		nbTransactionLimit     int
		startTransactionTicket chan bool
		endTransactionTicket   chan bool
	}
)

// Put add the given content to database with the given ID
func (c *Collection) Put(id string, content interface{}) error {
	c.startTransaction()
	defer c.endTransaction()

	if err := c.cleanRefs(id); err != nil {
		return err
	}

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

// Get retrieves the content of the given ID
func (c *Collection) Get(id string, pointer interface{}) ([]byte, error) {
	c.startTransaction()
	defer c.endTransaction()

	if id == "" {
		return nil, vars.ErrEmptyID
	}

	contentAsBytes := []byte{}

	response, getErr := c.get(id)
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

func (c *Collection) get(ids ...string) ([][]byte, error) {
	ret := make([][]byte, len(ids))
	if err := c.Store.View(func(txn *badger.Txn) error {
		for i, id := range ids {
			idAsBytes := c.buildStoreID(id)
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

			contentAsBytes, getValErr := item.Value()
			if getValErr != nil {
				return getValErr
			}
			ret[i] = contentAsBytes
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

// Delete removes the corresponding object if the given ID
func (c *Collection) Delete(id string) error {
	c.startTransaction()
	defer c.endTransaction()

	if id == "" {
		return vars.ErrEmptyID
	}

	if rmStoreErr := c.Store.Update(func(txn *badger.Txn) error {
		return txn.Delete(c.buildStoreID(id))
	}); rmStoreErr != nil {
		return rmStoreErr
	}

	return c.deleteIndexes(id)
}

// SetIndex enable the collection to index field or sub field
func (c *Collection) SetIndex(i *Index) error {
	c.startTransaction()
	defer c.endTransaction()

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

	i.getRangeIDsFunc = func(indexedValue []byte, keepEqual, increasing bool) (allIDs *IDs, err error) {
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
			if parseIDsErr != nil {
				return parseIDsErr
			}

			id := ID(idAsString)
			ids.AddID(&id)
			idsAsBytes = ids.MustMarshal()

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
func (c *Collection) Query(q *Query) (response *ResponseQuery, _ error) {
	if q == nil {
		return
	}

	if len(q.filters) <= 0 {
		return nil, fmt.Errorf("query has not get action")
	}

	c.startTransaction()
	defer c.endTransaction()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20000000)
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
		for _, filter := range q.filters {
			if index.DoesFilterApplyToIndex(filter) {
				go index.Query(ctx, filter, finishedChan)
				nbToDo++
			}
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

	fn, ret := iterator(q.limit)
	tree.Ascend(fn)

	response = NewResponseQuery(q.limit)
	response.query = q

	getObjectsFromStoreFunc := func(txn *badger.Txn) error {
		for i, id := range ret.IDs {
			objectsAsBadgeItem, getErr := txn.Get(c.buildStoreID(id.String()))
			if getErr != nil {
				if getErr == badger.ErrKeyNotFound {
					return vars.ErrNotFound
				}
				return getErr
			}
			objectsAsBytes, valErr := objectsAsBadgeItem.Value()
			if valErr != nil {
				return valErr
			}

			response.IDs[i] = id
			response.ObjectsAsBytes[i] = objectsAsBytes
		}
		return nil
	}

	if getValeErr := c.Store.View(getObjectsFromStoreFunc); getValeErr != nil {
		return nil, getValeErr
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

			indexBucket.Put(ref.IndexedValue, ids.MustMarshal())
		}

		return nil
	})
}
