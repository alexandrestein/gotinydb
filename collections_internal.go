package gotinydb

import (
	"fmt"

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

func (c *Collection) initTransactionTickets() {
	c.startTransactionTicket = make(chan bool, 0)
	c.endTransactionTicket = make(chan bool, c.nbTransactionLimit)

	go func() {
		for {
			if c.nbTransaction < c.nbTransactionLimit {
				select {
				case c.startTransactionTicket <- true:
					c.nbTransaction++
				case <-c.endTransactionTicket:
					c.nbTransaction--
				}
			} else {
				<-c.endTransactionTicket
				c.nbTransaction--
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

func (c *Collection) putIntoIndexes(id string, content interface{}) error {
	indexErrors := map[string]error{}
	for _, index := range c.Indexes {
		if val, apply := index.Apply(content); apply {
			putInIndexErr := index.setIDFunc(val, id)
			if putInIndexErr != nil {
				return putInIndexErr
			}
		}
	}

	if len(indexErrors) > 1 {
		errorString := "updating the index: "
		for indexName, err := range indexErrors {
			errorString += fmt.Sprintf("index %q: %s\n", indexName, err.Error())
		}
		return fmt.Errorf(errorString)
	}
	return nil
}

func (c *Collection) cleanRefs(idAsString string) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		refsBucket := tx.Bucket([]byte("refs"))
		return refsBucket.Delete(vars.BuildBytesID(idAsString))
	})
}
