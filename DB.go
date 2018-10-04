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

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transactions"
)

// Open simply opens a new or existing database
func Open(ctx context.Context, options *Options) (*DB, error) {
	d := new(DB)
	d.options = options
	d.ctx = ctx

	d.initWriteChannels(ctx)

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
	// Check if the crypto key has been updated
	cryptoChanged := false
	if d.options.CryptoKey != options.CryptoKey {
		cryptoChanged = true
		options.privateCryptoKey = d.options.privateCryptoKey
	}

	d.options = options

	// If the crypto key has been changed the config needs to be save with the new key
	if cryptoChanged {
		err := d.saveCollections()
		if err != nil {
			return err
		}
	}

	// Apply the configuration to all collections index stores
	for _, col := range d.collections {
		col.options = options
		// for _, index := range col.indexes {
		// 	index.options = options
		// }
	}
	return nil
}

// PutFile let caller insert large element into the database via a reader interface
func (d *DB) PutFile(id string, reader io.Reader) (n int, err error) {
	d.DeleteFile(id)

	// Track the numbers of chunks
	nChunk := 0
	// Open a loop
	for true {
		// init the context for transaction
		ctx, cancel := context.WithTimeout(d.ctx, d.options.TransactionTimeOut)
		defer cancel()

		// Initialize the read buffer
		buff := make([]byte, d.options.FileChunkSize)
		var nWritten int
		nWritten, err = reader.Read(buff)
		// The read is done and it returns
		if nWritten == 0 || err == io.EOF && nWritten == 0 {
			break
		}
		// Return error if any
		if err != nil && err != io.EOF {
			return
		}

		// Clean the buffer
		buff = buff[:nWritten]

		n = n + nWritten

		// Build the write element
		tr := transactions.NewTransaction(ctx)
		// trElem := newFileTransactionElement(id, nChunk, buff, true)
		trElem := transactions.NewTransactionElement(d.buildFilePrefix(id, nChunk), buff)
		tr.AddTransaction(trElem)

		// Run the insertion
		d.writeTransactionChan <- tr
		// And wait for the end of the insertion
		err = <-tr.ResponseChan
		if err != nil {
			return
		}

		// Increment the chunk counter
		nChunk++
	}

	err = nil
	return
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
			var err error
			var valAsEncryptedBytes []byte
			valAsEncryptedBytes, err = it.Item().ValueCopy(valAsEncryptedBytes)
			if err != nil {
				return err
			}

			var valAsBytes []byte
			valAsBytes, err = cipher.Decrypt(d.options.privateCryptoKey, it.Item().Key(), valAsEncryptedBytes)
			if err != nil {
				return err
			}

			_, err = writer.Write(valAsBytes)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// DeleteFile deletes every chunks of the given file ID
func (d *DB) DeleteFile(id string) error {
	// The list of chunk to delete
	ctx, cancel := context.WithTimeout(context.Background(), d.options.TransactionTimeOut)
	defer cancel()
	idsToDelete := transactions.NewTransaction(ctx)

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
			idsToDelete.AddTransaction(
				transactions.NewTransactionElement(key, nil),
			)
		}

		// Close the view transaction
		return nil
	})
	if err != nil {
		return err
	}

	// No need to open a new transaction if nothing needs to be removed
	if len(idsToDelete.Transactions) == 0 {
		return nil
	}

	d.writeTransactionChan <- idsToDelete

	return <-idsToDelete.ResponseChan

	// // Start the write operation and returns the error if any
	// return d.badgerDB.Update(func(txn *badger.Txn) error {
	// 	// Loop for every IDs to remove and remove it
	// 	for _, id := range idsToDelete {
	// 		err := txn.Delete(id)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// })
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
	for _, col := range d.collections {
		if col.name == collectionName {
			// Save the collection pointer for future cleanup
			c = col
			break
		}
	}

	// txn := d.badgerDB.NewTransaction(true)
	// defer txn.Discard()

	for {
		done, err := deleteLoop(d.badgerDB, c.getCollectionPrefix())
		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	// opt := badger.DefaultIteratorOptions
	// opt.PrefetchValues = false
	// it := txn.NewIterator(opt)
	// defer it.Close()

	// // Prevent panic
	// if c == nil {
	// 	return nil
	// }

	// // Remove the index DB files
	// prefix := c.buildCollectionPrefix()
	// for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
	// 	err := txn.Delete(it.Item().Key())
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// it.Close()

	// // Commit changes
	// err := txn.Commit(nil)
	// if err != nil {
	// 	return err
	// }

	// Put the prefix again into the free prefix list
	d.freeCollectionPrefixes = append(d.freeCollectionPrefixes, c.prefix)

	// Clean the in memory collections list
	for i, col := range d.collections {
		if col.name == collectionName {
			// Delete the collection form the list of collection pointers
			copy(d.collections[i:], d.collections[i+1:])
			d.collections[len(d.collections)-1] = nil
			d.collections = d.collections[:len(d.collections)-1]
			break
		}
	}

	return nil
}

// Backup run a badger.DB.Backup
func (d *DB) Backup(w io.Writer, since uint64) (uint64, error) {
	return d.badgerDB.Backup(w, since)
}

// Load restor the database from a backup file
func (d *DB) Load(r io.Reader) error {
	collection := d.collections
	d.collections = nil

	err := d.badgerDB.Load(r)
	if err != nil {
		d.collections = collection
		return err
	}

	err = d.loadCollections()
	if err != nil {
		d.collections = collection
		return err
	}

	for _, c := range d.collections {
		for _, i := range c.bleveIndexes {
			err = indexUnzipper(i.Path, i.IndexDirZip)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// GetCollections returns all collection pointers
func (d *DB) GetCollections() []*Collection {
	return d.collections
}
