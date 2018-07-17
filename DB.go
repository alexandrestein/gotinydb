/*
Package gotinydb provides a simple but powerfull NoSQL database.

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
			fmt.Println("ici", err)
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

// Backup run a backup to the given archive
func (d *DB) Backup(path string, since uint64) error {
	t0 := time.Now()
	file, openFileErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR, FilePermission)
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

// func (d *DB) ListBackup(path string) []uint64 {

// }

// func (d *DB) Load(path string, timestamp uint64) error {
// }

// func ()  {

// }

// func (d *DB) setConfig(file *os.File, since uint64) error {
// 	ret := new(archive)

// 	info, statErr := file.Stat()
// 	if statErr != nil {
// 		return nil, statErr
// 	}

// 	zipWriter, err := zip.NewWriter(file)
// 	if err != nil {
// 		return nil, err
// 	}

// 	archive.
// }

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

// func (d *DB) loadArchive(path string) (_ *archive, close func(), _ error) {
// 	file, openFileErr := os.OpenFile(path, os.O_CREATE|os.O_RDWR, FilePermission)
// 	if openFileErr != nil {
// 		return nil, nil, openFileErr
// 	}

// 	ret := new(archive)
// 	ret.file = file

// 	r, loadZipErr := d.getZipReader(file)
// 	if loadZipErr != nil {
// 		return nil, nil, loadZipErr
// 	}

// 	// Iterate through the files in the archive,
// 	// printing some of their contents.
// 	for _, f := range r.File {
// 		if f.Name == "archive.json" {
// 			fileAsReader, openFileErr := f.Open()
// 			if openFileErr != nil {
// 				return nil, nil, openFileErr
// 			}

// 			buf := make([]byte, 1000*100)
// 			n, readErr := fileAsReader.Read(buf)
// 			if readErr != nil {
// 				return nil, nil, readErr
// 			} else {
// 				buf = buf[:n]
// 			}

// 			unmarshalErr := json.Unmarshal(buf, ret)
// 			if unmarshalErr != nil {
// 				return nil, nil, unmarshalErr
// 			}

// 			return ret, nil, nil
// 		}
// 	}

// 	return ret, func() { file.Close() }, nil
// }

// func (d *DB) getZipReader(file *os.File) (*zip.Reader, error) {
// 	info, statErr := file.Stat()
// 	if statErr != nil {
// 		return nil, statErr
// 	}
// 	// Open a zip archive for reading.
// 	r, err := zip.NewReader(file, info.Size())
// 	if err != nil {
// 		return nil, err
// 	}

// 	return r, nil
// }
