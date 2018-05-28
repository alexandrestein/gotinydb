package GoTinyDB

import (
	"encoding/json"
	"fmt"

	"github.com/alexandreStein/GoTinyDB/collection"
	"github.com/alexandreStein/GoTinyDB/vars"
	bolt "github.com/coreos/bbolt"
)

func (d *DB) setNewCol(colName string) error {
	return d.boltDB.Update(func(tx *bolt.Tx) error {
		// Get the list of collection from internal bucket
		bucket := tx.Bucket(vars.InternalBuckectMetaDatas)
		v := bucket.Get(vars.InternalMetaDataCollectionsID)

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

		if setErr := bucket.Put(vars.InternalMetaDataCollectionsID, colNamesAsBytes); setErr != nil {
			return fmt.Errorf("saving the collection names: %s", setErr.Error())
		}

		return nil
	})
}

func (d *DB) updateCollection(col *collection.Collection) error {
	return d.boltDB.Update(func(tx *bolt.Tx) error {
		// Get the list of collection from internal bucket
		bucket := tx.Bucket(vars.InternalBuckectMetaDatas)

		colAsBytes, marshErr := json.Marshal(col)
		if marshErr != nil {
			return marshErr
		}

		if setErr := bucket.Put([]byte(col.Name), colAsBytes); setErr != nil {
			return fmt.Errorf("saving the collection names: %s", setErr.Error())
		}

		return nil
	})
}

func (d *DB) loadCollections() error {
	return d.boltDB.View(func(tx *bolt.Tx) error {
		// Get the list of collection from internal bucket
		v := tx.Bucket(vars.InternalBuckectMetaDatas).Get(vars.InternalMetaDataCollectionsID)

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
		v := tx.Bucket(vars.InternalBuckectMetaDatas).Get([]byte(name))

		col := new(collection.Collection)
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
