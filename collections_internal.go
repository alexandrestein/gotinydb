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
		if vars.BuildIDAsString(name) != id {
			return fmt.Errorf("ID and name not consistent. Name is %q with fingerprint %q but had %x", name, vars.BuildIDAsString(name), string(id))
		}
		c.ID = vars.BuildIDAsString(name)

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
			return fmt.Errorf("bucket error")
		}
		if err := confBucket.Put([]byte("name"), []byte(name)); err != nil {
			return err
		}
		if err := confBucket.Put([]byte("id"), []byte(vars.BuildIDAsString(name))); err != nil {
			return err
		}
		return nil
	})
}

func (c *Collection) buildStoreID(id string) []byte {
	compositeID := fmt.Sprintf("%s_%s", c.Name, id)
	return vars.BuildIDAsBytes(compositeID)
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
