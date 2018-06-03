package gotinydb

import (
	"fmt"

	"golang.org/x/crypto/blake2b"
)

func (d *DB) buildID(name string, col bool) []byte {
	prefix := ""
	if col {
		prefix = "col"
	} else {
		prefix = "index"
	}

	data := []byte(fmt.Sprintf("%s_%s", prefix, name))
	hash := blake2b.Sum512(data)
	return []byte(hash[:])
}
func (d *DB) buildIDString(name string, col bool) string {
	return fmt.Sprintf("%x", d.buildID(name, col))
}

func (d *DB) setNewCol(colName string) (*Collection, error) {
	db, getErr := d.getDB(d.buildIDString(colName, true))
	if getErr != nil {
		return nil, getErr
	}
	col := NewCollection(db, colName)
	// d.boltDB.Update(func(tx *bolt.Tx) error {
	// 	// Get the collections names
	// 	cols := getCollections(tx)
	// 	for name, _ := range cols {
	// 		if colName == name {
	// 			return fmt.Errorf("collection name %q allready exists", colName)
	// 		}
	// 	}
	//
	// 	col = NewCollection(d.boltDB, colName)
	// 	// Add metadata
	// 	setNamesErr := saveCollection(tx, col)
	// 	if setNamesErr != nil {
	// 		return setNamesErr
	// 	}
	// 	return nil
	// })
	// return col, nil

	return col, nil
}

// func (d *DB) updateCollection(col *Collection) error {
// 	return d.boltDB.Update(func(tx *bolt.Tx) error {
// 		bucket := tx.Bucket(InternalBuckectMetaDatas)
//
// 		colAsBytes, marshErr := json.Marshal(col)
// 		if marshErr != nil {
// 			return marshErr
// 		}
//
// 		if setErr := bucket.Put([]byte(col.Name+InternalMetaDataCollectionsIDSuffix), colAsBytes); setErr != nil {
// 			return fmt.Errorf("saving the collection name %q: %s", col.Name, setErr.Error())
// 		}
//
// 		return nil
// 	})
// }

// func (d *DB) loadCollections() error {
// 	// return d.boltDB.View(func(tx *bolt.Tx) error {
// 	// 	d.collections = getCollections(tx)
// 	//
// 	// 	return nil
// 	// 	})
// 	return nil
// }

// func (d *DB) loadCollection(name string) error {
// 	err := d.boltDB.View(func(tx *bolt.Tx) error {
// 		// The value is a JSON repesentation of the collection
// 		v := tx.Bucket(InternalBuckectMetaDatas).Get([]byte(name))
//
// 		col := new(Collection)
// 		if len(v) != 0 {
// 			uErr := json.Unmarshal(v, &col)
// 			if uErr != nil {
// 				return fmt.Errorf("unmarchaling collection names: %s", uErr.Error())
// 			}
// 		}
//
// 		d.collections[name] = col
//
// 		return nil
// 	})
//
// 	return err
// }
