package gotinydb

import (
	"encoding/json"
	"fmt"
	"os"

	bolt "github.com/coreos/bbolt"
)

// Open builds a new DB object with the given root path. It must be a directory.
// This path will be used to hold every elements. The entire data structur will
// be located in the directory.
func Open(path string) (*DB, error) {
	d := new(DB)
	d.collections = map[string]*Collection{}
	d.path = path

	if err := os.MkdirAll(path, FilePermission); err != nil {
		return nil, err
	}

	if err := d.loadCollections(); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DB) loadCollections() error {
	meta, getMetaErr := d.getMetaDB()
	if getMetaErr != nil {
		return getMetaErr
	}

	if err := meta.Update(func(tx *bolt.Tx) error {
		colBucket, createColBucketErr := tx.CreateBucketIfNotExists([]byte("collections"))
		if createColBucketErr != nil {
			return fmt.Errorf("can't make collections bucket: %s", createColBucketErr.Error())
		}

		curs := colBucket.Cursor()
		for k, v := curs.First(); k != nil; k, v = curs.Next() {
			collections := &Collection{}
			err := json.Unmarshal(v, collections)
			if err != nil {
				return err
			}
			return nil
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
func (d *DB) loadIndexes(curs *bolt.Cursor) error {
	for k, v := curs.First(); k != nil; k, v = curs.Next() {
		indexes := &structIndex{}
		err := json.Unmarshal(v, indexes)
		if err != nil {
			return err
		}
	}
	return nil
}

// func (d *DB) loadFromDB(key, value interface{})error   {
//
// }

// Use will try to get the collection from active ones. If not active it loads
// it from drive and if not existing it builds it.
func (d *DB) Use(colName string) (*Collection, error) {
	// Gets from in memory
	if activeCol, found := d.collections[colName]; found {
		// activeCol.SetBolt(d.boltDB)
		return activeCol, nil
	}

	col, err := d.setNewCol(colName)
	if err != nil {
		return nil, fmt.Errorf("setting the metadata: %s", err.Error())
	}

	// Save the collection into the object for future calls
	d.collections[colName] = col

	return col, nil
}

// Close removes the lock file
func (d *DB) Close() error {
	errors := "error closing bolt DB:\n"
	fail := false
	closeBolt := func(id string, err error) {
		if err != nil {
			fail = true
			fmt.Sprintf("%s%s: %s\n", errors, id, err)
		}
	}

	for _, col := range d.collections {
		closeBolt(col.Name, col.Close())
	}

	closeBolt("metadata DB", d.metaDB.Close())
	if fail {
		return fmt.Errorf(errors)
	}

	return nil
}

func (d *DB) getMetaDB() (*bolt.DB, error) {
	if d.metaDB == nil {
		bolt, err := d.getDB("meta")
		if err != nil {
			return nil, err
		}
		d.metaDB = bolt
	}

	return d.metaDB, nil
}

func (d *DB) getDB(id string) (*bolt.DB, error) {
	path := fmt.Sprintf("%s/%s", d.path, id)
	db, openBoltErr := bolt.Open(path, FilePermission, nil)
	if openBoltErr != nil {
		return nil, fmt.Errorf("openning bolt: %v", openBoltErr.Error())
	}
	return db, nil
}
