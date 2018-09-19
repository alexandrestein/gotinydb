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
}

// Use build or get a Collection pointer
func (d *DB) Use(colName string) (*Collection, error) {
	for _, col := range d.collections {
		if col.name == colName {
			return col, nil
		}
	}

	return d.initCollection(colName)
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

	d.collections = nil

	return d.loadCollections()
}

// GetCollections returns all collection pointers
func (d *DB) GetCollections() []*Collection {
	return d.collections
}
