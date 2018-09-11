/*
Package gotinydb provides a simple but powerful NoSQL database.

The goal is to have a simple way to store, order and retrieve values from storage.
It can handel big binnary files as structured objects with fields and subfields indexation.
*/
package gotinydb

import (
	"archive/zip"
	"compress/flate"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dgraph-io/badger"
)

// Open simply opens a new or existing database
func Open(ctx context.Context, options *Options) (*DB, error) {
	d := new(DB)
	d.options = options
	d.ctx = ctx

	if err := os.MkdirAll(d.options.Path, FilePermission); err != nil {
		return nil, err
	}

	if initBadgerErr := d.initBadger(); initBadgerErr != nil {
		return nil, initBadgerErr
	}
	if loadErr := d.loadCollections(); loadErr != nil {
		return nil, loadErr
	}

	d.initWriteTransactionChan(ctx)

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

	c, loadErr := d.getCollection(colName)
	if loadErr != nil {
		return nil, loadErr
	}

	if err := c.loadIndex(); err != nil {
		return nil, err
	}
	d.collections = append(d.collections, c)

	return c, nil
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

	// Remove the index DB files
	prefix := c.buildCollectionPrefix()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err := txn.Delete(it.Item().Key())
		if err != nil {
			return err
		}
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
				err := txn.Delete(c.buildStoreID(id.GetID()))
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

// Backup run a backup to the given archive
func (d *DB) Backup(path string, since uint64) error {
	t0 := time.Now()
	file, openFileErr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, FilePermission)
	if openFileErr != nil {
		return openFileErr
	}
	defer file.Close()

	zipWriter := zip.NewWriter(file)
	// Setup compression
	zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestCompression)
	})

	backupFile, createFileErr := zipWriter.Create("archive")
	if createFileErr != nil {
		return createFileErr
	}

	timestamp, backupErr := d.valueStore.Backup(backupFile, since)
	if backupErr != nil {
		return backupErr
	}

	configFile, createFileErr := zipWriter.Create("config.json")
	if createFileErr != nil {
		return createFileErr
	}

	archivePointer := d.loadArchive()
	archivePointer.StartTime = t0
	archivePointer.EndTime = time.Now()
	archivePointer.Timestamp = timestamp

	configAsBytes, marshalErr := json.Marshal(archivePointer)
	if marshalErr != nil {
		return marshalErr
	}

	_, writeErr := configFile.Write(configAsBytes)
	if writeErr != nil {
		return writeErr
	}

	return zipWriter.Close()
}

// Load restor the database from a backup file
func (d *DB) Load(path string) error {
	zipReader, openZipErr := zip.OpenReader(path)
	if openZipErr != nil {
		return openZipErr
	}

	config := new(archive)

	for _, file := range zipReader.File {
		switch file.Name {
		case "archive":
			reader, openErr := file.Open()
			if openErr != nil {
				return openErr
			}

			loadErr := d.valueStore.Load(reader)
			if loadErr != nil {
				return loadErr
			}
		case "config.json":
			configReader, openConfigReaderErr := file.Open()
			if openConfigReaderErr != nil {
				return openConfigReaderErr
			}

			decodeErr := json.NewDecoder(configReader).Decode(config)
			if decodeErr != nil {
				return decodeErr
			}
		}
	}

	// Add the indexes to the filledup database
	for _, collectionName := range config.Collections {
		collection, useCollectionErr := d.Use(collectionName)
		if useCollectionErr != nil {
			return useCollectionErr
		}
		for _, index := range config.Indexes[collectionName] {
			err := collection.SetIndex(index.Name, index.Type, index.Selector...)
			if err != nil {
				return err
			}
		}
	}
	return nil
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
