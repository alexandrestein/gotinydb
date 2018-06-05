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
		c.Name = name
		c.ID = fmt.Sprintf("%x", c.buildStoreID(name))

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

		tx.Bucket([]byte("config")).Put([]byte("name"), []byte(name))
		return nil
	})
}

func (c *Collection) buildStoreID(id string) []byte {
	compositeID := fmt.Sprintf("%s_%s", c.Name, id)
	return vars.BuildID(compositeID)
}

func (c *Collection) putIntoIndexes(id string, content interface{}) error {
	indexErrors := map[string]error{}
	for _, index := range c.Indexes {
		if val, apply := index.Apply(content); apply {
			fmt.Println("DO the indexing", string(val))
			// if updateErr := c.updateIndex(id, index, val); updateErr != nil {
			// 	indexErrors[indexName] = updateErr
			// }
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
