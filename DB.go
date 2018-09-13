/*
Package gotinydb provides a simple but powerful NoSQL database.

The goal is to have a simple way to store, order and retrieve values from storage.
It can handel big binnary files as structured objects with fields and subfields indexation.
*/
package gotinydb

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dgraph-io/badger"
)

// Open simply opens a new or existing database
func Open(ctx context.Context, options *Options) (*DB, error) {
	d := new(DB)
	d.options = options
	d.ctx = ctx

	d.initWriteTransactionChan(ctx)

	if err := os.MkdirAll(d.options.Path, FilePermission); err != nil {
		return nil, err
	}

	if initBadgerErr := d.initBadger(); initBadgerErr != nil {
		return nil, initBadgerErr
	}

	return d, d.loadCollections()
	// if loadErr := d.loadCollections(); loadErr != nil {
	// 	if loadErr == badger.ErrKeyNotFound {
	// 		err := d.initDB()
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 	} else {
	// 		return nil, loadErr
	// 	}
	// }

	// return d, nil
}

// Use build or get a Collection pointer
func (d *DB) Use(colName string) (*Collection, error) {
	for _, col := range d.collections {
		if col.name == colName {
			// if err := col.loadIndex(); err != nil {
			// 	return nil, err
			// }
			return col, nil
		}
	}

	return d.initCollection(colName)

	// c, loadErr := d.getCollection(colName)
	// if loadErr != nil {
	// 	return nil, loadErr
	// }

	// if err := c.loadIndex(); err != nil {
	// 	return nil, err
	// }
	// d.collections = append(d.collections, c)
	// return c, nil
}

// SetOptions update the database configurations.
// Some element won't apply before the database restart.
// For example the PutBufferLimit can't be change after the collection is started.
func (d *DB) SetOptions(options *Options) error {
	d.options = options

	// Apply the configuration to all collections index stores
	for _, col := range d.collections {
		col.options = options
		for _, index := range col.indexes {
			index.options = options
		}
	}
	return nil
}

// Close close the underneath collections and main store
func (d *DB) Close() error {
	if d.closing {
		return fmt.Errorf("already ongoing")
	}
	d.closing = true

	var err error
	if d.valueStore != nil {
		err = d.valueStore.Close()
	}

	d.options.Path = ""
	d.valueStore = nil
	d.collections = nil

	d = nil

	return err
}

// DeleteCollection delete the given collection
func (d *DB) DeleteCollection(collectionName string) error {
	var c *Collection
	for i, col := range d.collections {
		if col.name == collectionName {
			// Save the collection pointer for future cleanup
			c = col
			// Delete the collection form the list of collection pointers
			copy(d.collections[i:], d.collections[i+1:])
			d.collections[len(d.collections)-1] = nil
			d.collections = d.collections[:len(d.collections)-1]
			break
		}
	}

	txn := d.valueStore.NewTransaction(true)
	defer txn.Discard()
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false
	it := txn.NewIterator(opt)
	// Make sure that the iterator is closed.
	// But we have to make sure that close is called only onces
	// but we need to run it before commit.
	defer func() {
		if r := recover(); r != nil {
			it.Close()
		}
	}()

	// Prevent panic
	if c == nil {
		return nil
	}

	// Remove the index DB files
	prefix := c.buildCollectionPrefix()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := txn.Delete(it.Item().Key())
		if err != nil {
			return err
		}
	}
	it.Close()

	// // Remove stored values 1000 by 1000
	// for {
	// 	ids, err := c.getStoredIDsAndValues("", 1000, true)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if len(ids) == 0 {
	// 		break
	// 	}

	// 	for _, id := range ids {
	// 		err := txn.Delete(c.buildStoreID(id.GetID()))
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	// Commit changes
	err := txn.Commit(nil)
	if err != nil {
		return err
	}

	// Put the prefix again into the free prefix list
	d.freePrefix = append(d.freePrefix, c.prefix)

	return nil
}

// Backup run a badger.DB.Backup
func (d *DB) Backup(w io.Writer, since uint64) (uint64, error) {
	return d.valueStore.Backup(w, since)
}

// Load restor the database from a backup file
func (d *DB) Load(r io.Reader) error {
	err := d.valueStore.Load(r)
	if err != nil {
		return err
	}

	return d.loadCollections()

	// // Save elements
	// savedCtx := d.ctx
	// savedOptions1 := *d.options
	// savedOptions2 := *d.options

	// // Close the DB
	// err := d.Close()
	// if err != nil {
	// 	return err
	// }
	// // Remove all existing elements
	// os.RemoveAll(d.options.Path)

	// // Open a brand new database
	// var loadedDB *DB
	// loadedDB, err = Open(savedCtx, &savedOptions1)
	// if err != nil {
	// 	return err
	// }

	// // Load saved values
	// err = loadedDB.valueStore.Load(r)
	// if err != nil {
	// 	return err
	// }

	// // Close again
	// err = loadedDB.Close()
	// if err != nil {
	// 	return err
	// }

	// // And load a new collection
	// loadedDB, err = Open(savedCtx, &savedOptions2)
	// if err != nil {
	// 	return err
	// }
	// d = loadedDB

	// return nil
}

func (d *DB) loadArchive() *archive {
	ret := new(archive)
	ret.Collections = make([]string, len(d.collections))
	ret.Indexes = map[string][]*indexType{}

	for i, collection := range d.collections {
		ret.Collections[i] = collection.name

		ret.Indexes[collection.name] = make([]*indexType, len(collection.indexes))
		for j, index := range collection.indexes {
			ret.Indexes[collection.name][j] = index
		}
	}

	return ret
}

// GetCollections returns all collection pointers
func (d *DB) GetCollections() []*Collection {
	return d.collections
}
