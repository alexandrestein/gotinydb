/*
Package gotinydb implements a simple but useful embedded database.

It supports document insertion and retrieving of golang pointers via the JSON package.
Those documents can be indexed with Bleve.

File management is also supported and the all database is encrypted.

It relais on Bleve and Badger to do the job.
*/
package gotinydb

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"math"
	"os"
	"reflect"
	"runtime"
	"time"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/dgraph-io/badger/v2"
	"golang.org/x/crypto/blake2b"
)

type (
	// DB is the base struct of the package.
	// It provides the collection and manage all writes to the database.
	DB struct {
		ctx    context.Context
		cancel context.CancelFunc

		// Only used to save database settup
		configKey [32]byte
		// PrivateKey is public for marshaling reason and should never by used or changes.
		// This is the primary key used to derive every records.
		PrivateKey [32]byte

		path   string
		badger *badger.DB
		// Collection is public for marshaling reason and should never be used.
		// It contains the collections pointers used to manage the documents.
		Collections []*Collection

		// FileStore provides all accessibility to the file storage facilities
		FileStore *FileStore

		writeChan chan *transaction.Transaction
	}

	dbElement struct {
		Name string
		// Prefix defines the all prefix to the values
		Prefix []byte
	}
)

func init() {
	// This should prevent indexing the not indexed values
	mapping.StoreDynamic = false
	mapping.DocValuesDynamic = false
}

// Open initialize a new database or open an existing one.
// The path defines the place the data will be saved and the configuration key
// permit to decrypt existing configuration and to encrypt new one.
func Open(path string, configKey [32]byte) (db *DB, err error) {
	db = new(DB)
	db.path = path
	db.configKey = configKey
	db.ctx, db.cancel = context.WithCancel(context.Background())

	db.FileStore = &FileStore{db}

	options := badger.DefaultOptions
	options.Dir = path
	options.ValueDir = path

	options.MaxTableSize = int64(FileChuckSize) / 5     // 1MB
	options.ValueLogFileSize = int64(FileChuckSize) * 4 // 20MB
	options.NumCompactors = runtime.NumCPU()
	options.Truncate = true
	// Keep as much version as possible
	options.NumVersionsToKeep = math.MaxInt32

	db.writeChan = make(chan *transaction.Transaction, 1000)

	db.badger, err = badger.Open(options)
	if err != nil {
		return nil, err
	}
	db.startBackgroundLoops()

	err = db.loadConfig()
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return nil, err
		}
		// It's the first start of the database
		rand.Read(db.PrivateKey[:])
	} else {
		err = db.loadCollections()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (d *DB) startBackgroundLoops() {
	go d.goRoutineLoopForWrites()
	go d.goRoutineLoopForGC()
	go d.goWatchForTTLToClean()
}

// Use build a new collection or open an existing one.
func (d *DB) Use(colName string) (col *Collection, err error) {
	tmpHash := blake2b.Sum256([]byte(colName))
	prefix := append([]byte{prefixCollections}, tmpHash[:2]...)
	for _, savedCol := range d.Collections {
		if savedCol.Name == colName {
			if savedCol.db == nil {
				savedCol.db = d
			}
			col = savedCol
		} else if reflect.DeepEqual(savedCol.Prefix, prefix) {
			return nil, ErrHashCollision
		}
	}

	if col != nil {
		return col, nil
	}

	col = newCollection(colName)
	col.Prefix = prefix
	col.db = d

	d.Collections = append(d.Collections, col)

	err = d.saveConfig()
	if err != nil {
		return nil, err
	}

	return
}

// Close close the database and all subcomposants. It returns the error if any
func (d *DB) Close() (err error) {
	d.cancel()

	// In case of any error
	defer func() {
		if err != nil {
			d.badger.Close()
		}
	}()

	for _, col := range d.Collections {
		for _, i := range col.BleveIndexes {
			err = i.close()
			if err != nil {
				return err
			}
		}
	}

	return d.badger.Close()
}

// Backup perform a full backup of the database.
// It fills up the io.Writer with all data indexes and configurations.
func (d *DB) Backup(w io.Writer) error {
	_, err := d.badger.Backup(w, 0)
	return err
}

// GarbageCollection provides access to the garbage collection for the underneath database storeage (Badger).
//
// RunValueLogGC triggers a value log garbage collection.
//
// It picks value log files to perform GC based on statistics that are collected duing compactions. If no such statistics are available, then log files are picked in random order. The process stops as soon as the first log file is encountered which does not result in garbage collection.
// When a log file is picked, it is first sampled. If the sample shows that we can discard at least discardRatio space of that file, it would be rewritten.
// If a call to RunValueLogGC results in no rewrites, then an ErrNoRewrite is thrown indicating that the call resulted in no file rewrites.
// We recommend setting discardRatio to 0.5, thus indicating that a file be rewritten if half the space can be discarded. This results in a lifetime value log write amplification of 2 (1 from original write + 0.5 rewrite + 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer space reclaims, while setting it to a lower value would result in more space reclaims at the cost of increased activity on the LSM tree. discardRatio must be in the range (0.0, 1.0), both endpoints excluded, otherwise an ErrInvalidRequest is returned.
// Only one GC is allowed at a time. If another value log GC is running, or DB has been closed, this would return an ErrRejected.
// Note: Every time GC is run, it would produce a spike of activity on the LSM tree.
func (d *DB) GarbageCollection(discardRatio float64) error {
	if discardRatio <= 0 || discardRatio >= 1 {
		discardRatio = 0.5
	}

	return d.badger.RunValueLogGC(discardRatio)
}

// Load recover an existing database from a backup generated with *DB.Backup
func (d *DB) Load(r io.Reader) error {
	err := d.badger.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte{prefixConfig})
	})
	if err != nil {
		return err
	}

	err = d.badger.Load(r, 1000)
	if err != nil {
		return err
	}

	err = d.loadConfig()
	if err != nil {
		return err
	}

	for _, col := range d.Collections {
		col.db = d
		for _, index := range col.BleveIndexes {
			index.collection = col
			err = index.indexUnzipper()
			if err != nil {
				return err
			}
		}
	}

	return d.loadCollections()
}

func (d *DB) goRoutineLoopForGC() {
	ticker := time.NewTicker(time.Minute * 15)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// d.badger.Flatten(runtime.NumCPU())
			d.badger.RunValueLogGC(0.5)
		case <-d.ctx.Done():
			return
		}
	}
}

// This is where all writes are made
func (d *DB) goRoutineLoopForWrites() {
	limitNumbersOfWriteOperation := 10000
	limitSizeOfWriteOperation := 100 * 1000 * 1000 // 100MB
	limitWaitBeforeWriteStart := time.Millisecond * 50

	for {
		writeSizeCounter := 0

		var trans *transaction.Transaction
		var ok bool
		select {
		case trans, ok = <-d.writeChan:
			if !ok {
				return
			}
		case <-d.ctx.Done():
			return
		}

		// Save the size of the write
		writeSizeCounter += trans.GetWriteSize()
		firstArrivedAt := time.Now()

		// Add to the list of operation to be done
		waitingWrites := []*transaction.Transaction{trans}

		// Try to empty the queue if any
	tryToGetAnOtherRequest:
		select {
		// There is an other request in the queue
		case nextWrite := <-d.writeChan:
			// And save the response channel
			waitingWrites = append(waitingWrites, nextWrite)

			// Check if the limit is not reach
			if len(waitingWrites) < limitNumbersOfWriteOperation &&
				writeSizeCounter < limitSizeOfWriteOperation &&
				time.Since(firstArrivedAt) < limitWaitBeforeWriteStart {
				// If not lets try to empty the queue a bit more
				goto tryToGetAnOtherRequest
			}
			// This continue if there is no more request in the queue
		case <-d.ctx.Done():
			return
			// Stop waiting and do present operations
		default:
		}

		err := d.badger.Update(func(txn *badger.Txn) error {
			for _, transaction := range waitingWrites {
				for _, op := range transaction.Operations {
					var err error
					if op.Delete {
						err = txn.Delete(op.DBKey)
					} else {
						if op.CleanHistory {
							entry := badger.NewEntry(op.DBKey, cipher.Encrypt(d.PrivateKey, op.DBKey, op.Value))
							entry.WithDiscard()
							err = txn.SetEntry(entry)
						} else {
							err = txn.Set(op.DBKey, cipher.Encrypt(d.PrivateKey, op.DBKey, op.Value))
						}
					}

					// Returns the write error to the caller
					if err != nil {
						go d.nonBlockingResponseChan(transaction, err)
					}

				}
			}
			return nil
		})

		// Dispatch the commit response to all callers
		for _, op := range waitingWrites {
			go d.nonBlockingResponseChan(op, err)
		}
	}
}

func (d *DB) nonBlockingResponseChan(tx *transaction.Transaction, err error) {
	select {
	case tx.ResponseChan <- err:
	case <-d.ctx.Done():
	case <-tx.Ctx.Done():
	}
}

func (d *DB) decryptData(dbKey, encryptedData []byte) (clear []byte, err error) {
	return cipher.Decrypt(d.PrivateKey, dbKey, encryptedData)
}

// saveConfig save the database configuration with collections and indexes
func (d *DB) saveConfig() (err error) {
	return d.badger.Update(func(txn *badger.Txn) error {
		// Convert to JSON
		dbToSaveAsBytes, err := json.Marshal(d)
		if err != nil {
			return err
		}

		dbKey := []byte{prefixConfig}
		e := &badger.Entry{
			Key:   dbKey,
			Value: cipher.Encrypt(d.configKey, dbKey, dbToSaveAsBytes),
		}

		return txn.SetEntry(e)
	})
}

func (d *DB) getConfig() (db *DB, err error) {
	err = d.badger.View(func(txn *badger.Txn) error {
		dbKey := []byte{prefixConfig}

		var item *badger.Item
		item, err = txn.Get(dbKey)
		if err != nil {
			return err
		}

		var dbAsBytes []byte
		dbAsBytes, err = item.ValueCopy(dbAsBytes)
		if err != nil {
			return err
		}

		dbAsBytes, err = cipher.Decrypt(d.configKey, dbKey, dbAsBytes)
		if err != nil {
			return err
		}

		db = new(DB)
		return json.Unmarshal(dbAsBytes, db)
	})

	if db != nil {
		db.configKey = d.configKey
	}

	return
}

func (d *DB) loadConfig() error {
	db, err := d.getConfig()
	if err != nil {
		return err
	}

	d.cancel()

	time.Sleep(time.Millisecond * 500)

	// db.cancel = d.cancel
	db.badger = d.badger
	db.ctx, db.cancel = context.WithCancel(context.Background())
	db.writeChan = d.writeChan
	db.path = d.path
	db.FileStore = d.FileStore

	*d = *db

	d.startBackgroundLoops()

	return nil
}

func (d *DB) loadCollections() (err error) {
	for _, col := range d.Collections {
		for _, index := range col.BleveIndexes {
			index.collection = col
			indexPrefix := make([]byte, len(index.Prefix))
			copy(indexPrefix, index.Prefix)
			config := blevestore.NewConfigMap(d.ctx, index.Path, d.PrivateKey, indexPrefix, d.badger, d.writeChan)
			index.bleveIndex, err = bleve.OpenUsing(d.path+string(os.PathSeparator)+index.Path, config)
			if err != nil {
				return
			}
		}
	}
	return
}

// DeleteCollection removes every document and indexes and the collection itself
func (d *DB) DeleteCollection(colName string) {
	var col *Collection
	for i, tmpCol := range d.Collections {
		if tmpCol.Name == colName {
			col = tmpCol

			copy(d.Collections[i:], d.Collections[i+1:])
			d.Collections[len(d.Collections)-1] = nil // or the zero value of T
			d.Collections = d.Collections[:len(d.Collections)-1]

			break
		}
	}

	for _, index := range col.BleveIndexes {
		index.close()
		index.delete()
	}

	d.deletePrefix(col.Prefix)
}

func (d *DB) deletePrefix(prefix []byte) error {
	return d.badger.DropPrefix(prefix)
}
