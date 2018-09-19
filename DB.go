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

// PutFile let caller insert large element into the database via a reader interface
func (d *DB) PutFile(id string, reader io.Reader) error {
	// Track the numbers of chunks
	nChunk := 0
	// Open a loop
	for true {
		// init the context for transaction
		ctx, cancel := context.WithTimeout(d.ctx, d.options.TransactionTimeOut)
		defer cancel()

		// Initialize the read buffer
		buff := make([]byte, d.options.FileChunkSize)
		nWritten, err := reader.Read(buff)
		// The read is done and it returns
		if nWritten == 0 || err == io.EOF && nWritten == 0 {
			return nil
		}
		// Return error if any
		if err != nil && err != io.EOF {
			return err
		}

		// Clean the buffer
		buff = buff[:nWritten]

		// Build the write element
		tr := newTransaction(ctx)
		trElem := newFileTransactionElement(id, nChunk, buff, true)
		tr.addTransaction(trElem)

		// Run the insertion
		d.writeTransactionChan <- tr
		// And wait for the end of the insertion
		err = <-tr.responseChan
		if err != nil {
			return err
		}

		// Increment the chunk counter
		nChunk++
	}

	return nil
}

// ReadFile write file content into the given writer
func (d *DB) ReadFile(id string, writer io.Writer) error {
	return d.badgerDB.View(func(txn *badger.Txn) error {
		storeID := d.buildFilePrefix(id, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchSize = 3
		opt.PrefetchValues = true

		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			val, err := it.Item().Value()
			if err != nil {
				return err
			}

			var n int
			n, err = writer.Write(val)
			if err != nil {
				return err
			}
			if n != len(val) {
				return fmt.Errorf("the writer did not write the same number of byte but did not return error. writer returned %d but the value is %d byte length", n, len(val))
			}
		}

		return nil
	})
}

// DeleteFile deletes every chunks of the given file ID
func (d *DB) DeleteFile(id string) error {
	// The list of chunk to delete
	idsToDelete := [][]byte{}

	// Open a read transaction to get every IDs
	err := d.badgerDB.View(func(txn *badger.Txn) error {
		// Build the file prefix
		storeID := d.buildFilePrefix(id, -1)

		// Defines the iterator options to get only IDs
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		// Initialize the iterator
		it := txn.NewIterator(opt)
		defer it.Close()

		// Go the the first file chunk
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			// Copy the store key
			var key []byte
			key = it.Item().KeyCopy(key)
			// And add it to the list of store IDs to delete
			idsToDelete = append(idsToDelete, key)
		}

		// Close the view transaction
		return nil
	})
	if err != nil {
		return err
	}

	// Start the write operation and returns the error if any
	return d.badgerDB.Update(func(txn *badger.Txn) error {
		// Loop for every IDs to remove and remove it
		for _, id := range idsToDelete {
			err := txn.Delete(id)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Close close the underneath collections and main store
func (d *DB) Close() error {
	if d.closing {
		return fmt.Errorf("already ongoing")
	}
	d.closing = true

	var err error
	if d.badgerDB != nil {
		err = d.badgerDB.Close()
	}

	d.options.Path = ""
	d.badgerDB = nil
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

	txn := d.badgerDB.NewTransaction(true)
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
	return d.badgerDB.Backup(w, since)
}

// Load restor the database from a backup file
func (d *DB) Load(r io.Reader) error {
	err := d.badgerDB.Load(r)
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
