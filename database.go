package gotinydb

import (
	"fmt"

	bolt "github.com/coreos/bbolt"
)

// Open builds a new DB object with the given root path. It must be a directory.
// This path will be used to hold every elements. The entire data structur will
// be located in the directory.
func Open(path string) (*DB, error) {
	d := new(DB)
	d.collections = map[string]*Collection{}
	d.path = path

	// lockFile, addLockErr := os.OpenFile(d.path+"/"+vars.LockFileName, vars.OpenDBFlags, vars.FilePermission)
	// if addLockErr != nil {
	// 	return nil, fmt.Errorf("setting lock: %s", addLockErr.Error())
	// }
	// d.lockFile = lockFile

	bolt, openBoltErr := bolt.Open(path, FilePermission, nil)
	if openBoltErr != nil {
		return nil, fmt.Errorf("openning bolt: %v", openBoltErr.Error())
	}
	d.boltDB = bolt

	if err := d.checkAndBuildInternalBuckets(); err != nil {
		return nil, fmt.Errorf("checking internal buckets %s", err.Error())
	}
	if err := d.loadCollections(); err != nil {
		return nil, fmt.Errorf("loading internal buckets: %s", err.Error())
	}

	return d, nil
}

func (d *DB) checkAndBuildInternalBuckets() error {
	return d.boltDB.Update(func(tx *bolt.Tx) error {
		getInternalMetaBucket(tx)
		getInternalCollectionsBucket(tx)

		// if _, err := tx.CreateBucketIfNotExists(InternalBuckectMetaDatas); err != nil {
		// 	return err
		// }
		// if _, err := tx.CreateBucketIfNotExists(InternalBuckectCollections); err != nil {
		// 	return err
		// }
		return nil
	})
}

// Use will try to get the collection from active ones. If not active it loads
// it from drive and if not existing it builds it.
func (d *DB) Use(colName string) (*Collection, error) {
	// Gets from in memory
	if activeCol, found := d.collections[colName]; found {
		// activeCol.SetBolt(d.boltDB)
		return activeCol, nil
	}

	// // Build a new collection
	// col := NewCollection(d.boltDB, colName)

	col, err := d.setNewCol(colName)
	if err != nil {
		return nil, fmt.Errorf("setting the metadata: %s", err.Error())
	}

	// if err := d.updateCollection(col); err != nil {
	// 	return nil, fmt.Errorf("setting the collection: %s", err.Error())
	// }

	// Save the collection into the object for future calls
	d.collections[colName] = col

	return col, nil

	// // Gets from drive
	// col, openColErr := collection.NewCollection(d.getPathFor(colName))
	// if openColErr != nil {
	// 	return nil, fmt.Errorf("loading collection: %s", openColErr.Error())
	// }
	//
	// // Save the collection in memory
	// d.Collections[colName] = col
	//
	// return col, nil
}

// Close removes the lock file
func (d *DB) Close() {
	d.boltDB.Close()
	// os.Remove(d.path + "/" + vars.LockFileName)
}

// // CloseCollection clean the collection slice of the object of the collection
// func (d *DB) CloseCollection(colName string) {
// 	delete(d.collections, colName)
// }
//
// func (d *DB) getPathFor(colName string) string {
// 	return fmt.Sprintf("%s/%s", d.path, colName)
// }
//
// func (d *DB) buildRootDir() error {
// 	if makeRootDirErr := os.MkdirAll(d.path, vars.FilePermission); makeRootDirErr != nil {
// 		if os.IsExist(makeRootDirErr) {
// 			return nil
// 		}
// 		return fmt.Errorf("building root directory: %s", makeRootDirErr.Error())
// 	}
// 	return nil
// }
