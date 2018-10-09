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
	DB struct {
		ctx    context.Context
		cancel context.CancelFunc

		ConfigKey  [32]byte `json:"-"`
		PrivateKey [32]byte

		Path        string     `json:"-"`
		Badger      *badger.DB `json:"-"`
		Collections []*Collection

		writeChan chan *transaction.Transaction
	}

	dbElement struct {
		Name string
		// Prefix defines the all prefix to the values
		Prefix []byte
	}
)

func Open(path string, configKey [32]byte) (db *DB, err error) {
	db = new(DB)
	db.Path = path
	db.ConfigKey = configKey
	db.ctx, db.cancel = context.WithCancel(context.Background())

	options := badger.DefaultOptions
	options.Dir = path
	options.ValueDir = path
	// Keep as much version as possible
	options.NumVersionsToKeep = math.MaxInt32

	db.writeChan = make(chan *transaction.Transaction, 1000)
	go db.goRoutineLoopForWrites()

	db.Badger, err = badger.Open(options)
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

	col = NewCollection(colName)
	col.Prefix = prefix
	col.db = d

	d.Collections = append(d.Collections, col)

	err = d.saveConfig()
	if err != nil {
		return nil, err
	}

	return
}

func (d *DB) Close() (err error) {
	d.cancel()

	// In case of any error
	defer func() {
		if err != nil {
			d.Badger.Close()
		}
	}()

	for _, col := range d.Collections {
		for _, i := range col.BleveIndexes {
			err = i.Close()
			if err != nil {
				return err
			}
		}
	}

	return d.Badger.Close()
}

func (d *DB) Backup(w io.Writer) error {
	_, err := d.Badger.Backup(w, 0)
	return err
}
func (d *DB) Load(r io.Reader) error {
	err := d.Badger.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte{prefixConfig})
	})
	if err != nil {
		return err
	}

	err = d.Badger.Load(r)
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

		err := d.Badger.Update(func(txn *badger.Txn) error {
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
	select {
	case tx.ResponseChan <- err:
	case <-d.ctx.Done():
	case <-tx.Ctx.Done():
	}
}

func (d *DB) decryptData(dbKey, encryptedData []byte) (clear []byte, err error) {
	return cipher.Decrypt(d.PrivateKey, dbKey, encryptedData)
}

func (d *DB) saveConfig() (err error) {
	return d.Badger.Update(func(txn *badger.Txn) error {
		// Convert to JSON
		dbToSaveAsBytes, err := json.Marshal(d)
		if err != nil {
			return err
		}

		dbKey := []byte{prefixConfig}
		e := &badger.Entry{
			Key:   dbKey,
			Value: cipher.Encrypt(d.ConfigKey, dbKey, dbToSaveAsBytes),
		}

		return txn.SetEntry(e)
	})
}

func (d *DB) loadConfig() (err error) {
	return d.Badger.View(func(txn *badger.Txn) error {
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

		dbAsBytes, err = cipher.Decrypt(d.ConfigKey, dbKey, dbAsBytes)
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
			config := blevestore.NewBleveStoreConfigMap(d.ctx, index.Path, d.PrivateKey, indexPrefix, d.Badger, d.writeChan)
			index.BleveIndex, err = bleve.OpenUsing(index.Path, config)
			if err != nil {
				return
			}
		}
	}
	return
}
