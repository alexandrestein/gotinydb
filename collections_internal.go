package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/boltdb/bolt"
)

func (c *Collection) loadInfos() error {
	return c.DB.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("config"))
		if bucket == nil {
			return vars.ErrNotFound
		}

		name := string(bucket.Get([]byte("name")))
		id := string(bucket.Get([]byte("id")))
		c.Name = name
		c.ID = string(id)

		return nil
	})
}

func (c *Collection) init(name string) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		bucketsToCreate := []string{"config", "indexes", "refs"}
		for _, bucketName := range bucketsToCreate {
			if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
				return err
			}
		}

		confBucket := tx.Bucket([]byte("config"))
		if confBucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		if err := confBucket.Put([]byte("name"), []byte(name)); err != nil {
			return err
		}
		if err := confBucket.Put([]byte("id"), []byte(c.ID)); err != nil {
			return err
		}
		return nil
	})
}

func (c *Collection) initTransactionTickets(ctx context.Context) {
	c.startTransactionTicket = make(chan bool, 0)
	c.endTransactionTicket = make(chan bool, c.nbTransactionLimit)

	go func() {
		for {
			if c.nbTransaction < c.nbTransactionLimit {
				select {
				case c.startTransactionTicket <- true:
					// Unlock the caller of Collection.startTransaction
					c.nbTransaction++
				case <-c.endTransactionTicket:
					// In case a transaction is done
					c.nbTransaction--
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case <-c.endTransactionTicket:
					c.nbTransaction--
				case <-ctx.Done():
					return
				}
			}
		}
	}()
}

func (c *Collection) startTransaction() {
	<-c.startTransactionTicket
}
func (c *Collection) endTransaction() {
	c.endTransactionTicket <- true
}

func (c *Collection) buildStoreID(id string) []byte {
	compositeID := fmt.Sprintf("%s_%s", c.Name, id)
	objectID := vars.BuildID(compositeID)
	return []byte(fmt.Sprintf("%s_%s", c.ID[:4], objectID))
}

func (c *Collection) putIntoIndexes(ctx context.Context, storeErr, indexErr chan error, id string, content interface{}) error {
	runingIndex := 0
	internalIndexErr := make(chan error, 8)

	for _, index := range c.Indexes {
		if val, apply := index.Apply(content); apply {
			runingIndex++
			go index.setIDFunc(ctx, storeErr, internalIndexErr, val, id)
		}
	}

	for {
		select {
		case _, ok := <-internalIndexErr:
			if !ok {
				return nil
			}

			runingIndex--
			if runingIndex <= 0 {
				close(indexErr)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		time.Sleep(time.Millisecond)
	}
}

func (c *Collection) cleanRefs(ctx context.Context, idAsString string) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		indexBucket := tx.Bucket([]byte("indexes"))
		refsBucket := tx.Bucket([]byte("refs"))

		// Get the references of the given ID
		refsAsBytes := refsBucket.Get(vars.BuildBytesID(idAsString))
		refs := NewRefs()
		if refsAsBytes == nil && len(refsAsBytes) > 0 {
			if err := json.Unmarshal(refsAsBytes, refs); err != nil {
				return err
			}
		}

		// Clean every reference of the object In all indexes if present
		for _, ref := range refs.Refs {
			for _, index := range c.Indexes {
				if index.Name == ref.IndexName {
					// If reference present in this index the reference is cleaned
					ids, newIDErr := NewIDs(indexBucket.Bucket([]byte(index.Name)).Get(ref.IndexedValue))
					if newIDErr != nil {
						return newIDErr
					}
					ids.RmID(idAsString)
					// And saved again after the clean
					if err := indexBucket.Bucket([]byte(index.Name)).Put(ref.IndexedValue, ids.MustMarshal()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}
