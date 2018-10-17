package gotinydb

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"
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

		writeChan chan *transaction.Transaction
	}

	dbElement struct {
		Name string
		// Prefix defines the all prefix to the values
		Prefix []byte
	}
)

// Open initialize a new database or open an existing one.
// The path defines the place the data will be saved and the configuration key
// permit to decrypt existing configuration and to encrypt new one.
func Open(path string, configKey [32]byte) (db *DB, err error) {
	db = new(DB)
	db.path = path
	db.configKey = configKey
	db.ctx, db.cancel = context.WithCancel(context.Background())

	options := badger.DefaultOptions
	options.Dir = path
	options.ValueDir = path
	// Keep as much version as possible
	options.NumVersionsToKeep = math.MaxInt32

	db.writeChan = make(chan *transaction.Transaction, 1000)
	go db.goRoutineLoopForWrites()

	db.badger, err = badger.Open(options)
	if err != nil {
		return nil, err
	}

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
		} else if reflect.DeepEqual(col.Prefix, prefix) {
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

// Load recover an existing database from a backup generated with *DB.Backup
func (d *DB) Load(r io.Reader) error {
	err := d.badger.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte{prefixConfig})
	})
	if err != nil {
		return err
	}

	err = d.badger.Load(r)
	if err != nil {
		return err
	}

	err = d.loadConfig()
	if err != nil {
		return err
	}

	for _, col := range d.Collections {
		for _, index := range col.BleveIndexes {
			index.indexUnzipper()
		}
	}

	return d.loadCollections()
}

// This is where all writes are made
func (d *DB) goRoutineLoopForWrites() {
	limitNumbersOfWriteOperation := 10000
	limitSizeOfWriteOperation := 100 * 1000 * 1000 // 100MB
	limitWaitBeforeWriteStart := time.Millisecond * 50

	for {
		writeSizeCounter := 0

		var op *transaction.Transaction
		var ok bool
		select {
		case op, ok = <-d.writeChan:
			if !ok {
				return
			}
		case <-d.ctx.Done():
			return
		}

		// Save the size of the write
		writeSizeCounter += len(op.Value)
		firstArrivedAt := time.Now()

		// Add to the list of operation to be done
		waitingWrites := []*transaction.Transaction{op}

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
			for _, op := range waitingWrites {
				var err error
				if op.Delete {
					// fmt.Println("delete", op.DBKey)
					err = txn.Delete(op.DBKey)
				} else if op.CleanHistory {
					err = txn.SetWithDiscard(op.DBKey, cipher.Encrypt(d.PrivateKey, op.DBKey, op.Value), 0)
				} else {
					// fmt.Println("write", op.DBKey)
					err = txn.Set(op.DBKey, cipher.Encrypt(d.PrivateKey, op.DBKey, op.Value))
				}
				// Returns the write error to the caller
				if err != nil {
					go d.nonBlockingResponseChan(op, err)
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
	if tx.ResponseChan == nil {
		return
	}
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

func (d *DB) loadConfig() (err error) {
	return d.badger.View(func(txn *badger.Txn) error {
		dbKey := []byte{prefixConfig}

		item, err := txn.Get(dbKey)
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

		return json.Unmarshal(dbAsBytes, d)
	})
}

func (d *DB) loadCollections() (err error) {
	for _, col := range d.Collections {
		for _, index := range col.BleveIndexes {
			indexPrefix := make([]byte, len(index.Prefix))
			copy(indexPrefix, index.Prefix)
			config := blevestore.NewBleveStoreConfigMap(d.ctx, index.Path, d.PrivateKey, indexPrefix, d.badger, d.writeChan)
			index.bleveIndex, err = bleve.OpenUsing(index.Path, config)
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

func (d *DB) deletePrefix(prefix []byte) {
	// Wait for write to be done in case any
	time.Sleep(time.Millisecond * 500)

	finished := false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

newLoop:
	idToDelete := []*transaction.Transaction{}

	d.badger.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		iter := txn.NewIterator(opt)
		defer iter.Close()

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			var key []byte
			key = item.KeyCopy(key)

			tx := transaction.NewTransaction(ctx, key, nil, true)
			idToDelete = append(idToDelete, tx)

			if len(idToDelete) > 10000 {
				return nil
			}
		}

		finished = true

		return nil
	})

	for _, tx := range idToDelete {
		d.writeChan <- tx
		close(tx.ResponseChan)
		tx.ResponseChan = nil
	}

	if !finished {
		time.Sleep(time.Millisecond * 500)
		goto newLoop
	}
}

