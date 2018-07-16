/*
Package gotinydb provides a simple but powerfull NoSQL database.

The goal is to have a simple way to store, order and retrieve values from storage.
It can handel big binnary files as structured objects with fields and subfields indexation.
*/
package gotinydb

import (
	"context"
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
)

// Open simply opens a new or existing database
func Open(ctx context.Context, options *Options) (*DB, error) {
	d := new(DB)
	d.options = options
	d.ctx = ctx

	d.options = options

	if err := d.buildPath(); err != nil {
		return nil, err
	}

	if initBadgerErr := d.initBadger(); initBadgerErr != nil {
		return nil, initBadgerErr
	}

	if loadErr := d.loadCollections(); loadErr != nil {
		return nil, loadErr
	}

	go d.waitForClose()

	return d, nil
}

// Use build or get a Collection pointer
func (d *DB) Use(colName string) (*Collection, error) {
	for _, col := range d.collections {
		if col.name == colName {
			if err := col.loadIndex(); err != nil {
				return nil, err
			}
			return col, nil
		}
	}

	c, loadErr := d.getCollection("", colName)
	if loadErr != nil {
		return nil, loadErr
	}

	if err := c.loadIndex(); err != nil {
		return nil, err
	}
	d.collections = append(d.collections, c)

	return c, nil
}

// SetOptions update the database configurations
func (d *DB) SetOptions(options *Options) error {
	d.options = options

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

	errors := ""
	for i, col := range d.collections {
		if err := col.db.Close(); err != nil {
			errors = fmt.Sprintf("%s%s\n", errors, err.Error())
		}
		d.collections[i] = nil
	}

	if d.valueStore != nil {
		err := d.valueStore.Close()
		if err != nil {
			errors = fmt.Sprintf("%s%s\n", errors, err.Error())
		}
	}

	if errors != "" {
		return fmt.Errorf(errors)
	}

	d.options.Path = ""
	d.valueStore = nil
	d.collections = nil

	d = nil
	return nil
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

	// Close index DB
	if err := c.db.Close(); err != nil {
		return err
	}
	// Remove the index DB files
	if err := os.RemoveAll(d.options.Path + "/collections/" + c.id); err != nil {
		return err
	}

	// Remove stored values 1000 by 1000
	for {
		ids, err := c.getStoredIDsAndValues("", 1000, true)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}

		err = d.valueStore.Update(func(txn *badger.Txn) error {
			for _, id := range ids {
				err := txn.Delete(c.buildStoreID(id.ID.ID))
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
}
