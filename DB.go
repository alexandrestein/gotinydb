package gotinydb

import (
	"encoding/json"
	"fmt"

	bolt "github.com/coreos/bbolt"
)

func (d *DB) setNewCol(colName string) error {
	return d.boltDB.Update(func(tx *bolt.Tx) error {
		// Get the list of collection from internal bucket
		metaDataBucket := tx.Bucket(InternalBuckectMetaDatas)

		if _, creatColMetaBucketErr := metaDataBucket.CreateBucket([]byte(colName)); creatColMetaBucketErr != nil {
			return fmt.Errorf("can't create the meta data bucket for collection %q: %s", colName, creatColMetaBucketErr.Error())
		}

		v := metaDataBucket.Get(InternalMetaDataCollectionsID)

		collectionsNames := []string{}
		// If the response is empty there is no existing collections.
		if len(v) != 0 {
			uErr := json.Unmarshal(v, collectionsNames)
			if uErr != nil {
				return fmt.Errorf("unmarshaling collections names: %s", uErr.Error())
			}
		}

		collectionsNames = append(collectionsNames, colName)

		colNamesAsBytes, marshErr := json.Marshal(collectionsNames)
		if marshErr != nil {
			return fmt.Errorf("marshaling index names: %s", marshErr.Error())
		}

		if setErr := metaDataBucket.Put(InternalMetaDataCollectionsID, colNamesAsBytes); setErr != nil {
			return fmt.Errorf("saving the collection names: %s", setErr.Error())
		}

		return nil
	})
}

func (d *DB) updateCollection(col *Collection) error {
	return d.boltDB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(InternalBuckectMetaDatas)

		colAsBytes, marshErr := json.Marshal(col)
		if marshErr != nil {
			return marshErr
		}

		if setErr := bucket.Put([]byte(col.Name+InternalMetaDataCollectionsIDSuffix), colAsBytes); setErr != nil {
			return fmt.Errorf("saving the collection name %q: %s", col.Name, setErr.Error())
		}

		return nil
	})
}

func (d *DB) loadCollections() error {
	return d.boltDB.View(func(tx *bolt.Tx) error {
		// Get the list of collection from internal bucket
		v := tx.Bucket(InternalBuckectMetaDatas).Get(InternalMetaDataCollectionsID)

		// If the response is empty there is no existing collections.
		if len(v) == 0 {
			return nil
		}

		indexNames := []string{}
		uErr := json.Unmarshal(v, &indexNames)
		if uErr != nil {
			return fmt.Errorf("unmarshaling collections names %s: %s", string(v), uErr.Error())
		}

		for _, indexName := range indexNames {
			if err := d.loadCollection(indexName); err != nil {
				return err
			}
		}

		return nil
	})
}

func (d *DB) loadCollection(name string) error {
	err := d.boltDB.View(func(tx *bolt.Tx) error {
		// The value is a JSON repesentation of the collection
		v := tx.Bucket(InternalBuckectMetaDatas).Get([]byte(name))

		col := new(Collection)
		if len(v) != 0 {
			uErr := json.Unmarshal(v, &col)
			if uErr != nil {
				return fmt.Errorf("unmarchaling collection names: %s", uErr.Error())
			}
		}

		d.collections[name] = col

		return nil
	})

	return err
}
