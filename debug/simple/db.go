package simple

import (
	"encoding/json"
	"reflect"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/debug/simple/transaction"
	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

type (
	DB struct {
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

	options := badger.DefaultOptions
	options.Dir = path
	options.ValueDir = path

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
	} else {
		for _, col := range db.Collections {
			for _, index := range col.BleveIndexes {
				config := blevestore.NewBleveStoreConfigMap(index.Path, db.PrivateKey, col.Prefix, db.Badger, db.writeChan)
				index.BleveIndex, err = bleve.OpenUsing(index.Path, config)
				if err != nil {
					return
				}
			}
		}
	}

	return db, nil
}

func (d *DB) Use(colName string) (col *Collection, err error) {
	tmpHash := blake2b.Sum256([]byte(colName))
	prefix := append([]byte{prefixCollections}, tmpHash[:2]...)
	for _, col = range d.Collections {
		if col.Name == colName {
			if col.db == nil {
				col.db = d
			}
			return
		}
		if reflect.DeepEqual(col.Prefix, prefix) {
			return nil, ErrHashCollision
		}
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

func (d *DB) goRoutineLoopForWrites() {
	for {
		ops, ok := <-d.writeChan
		if !ok {
			return
		}

		waitingWrites := []*transaction.Transaction{ops}

		// Try to empty the queue if any
	tryToGetAnOtherRequest:
		select {
		// There is an other request in the queue
		case nextWrite := <-d.writeChan:
			// And save the response channel
			waitingWrites = append(waitingWrites, nextWrite)

			// Check if the limit is not reach
			if len(waitingWrites) < 10000 {
				// If not lets try to empty the queue a bit more
				goto tryToGetAnOtherRequest
			}
			// This continue if there is no more request in the queue
		default:
		}

		err := db.Badger.Update(func(txn *badger.Txn) error {
			for _, op := range waitingWrites {
				var err error
				if op.Delete {
					// fmt.Println("delete", op.DBKey, string(op.DBKey))
					err = txn.Delete(op.DBKey)
				} else {
					// fmt.Println("write", op.DBKey, string(op.DBKey))
					err = txn.Set(op.DBKey, cipher.Encrypt(db.PrivateKey, op.DBKey, op.Value))
				}
				if err != nil {
					go d.nonBlockingResponseChan(op.ResponseChan, err)
				}
			}
			return nil
		})

		// Dispatch the commit response
		for _, op := range waitingWrites {
			go d.nonBlockingResponseChan(op.ResponseChan, err)
		}
	}
}

func (d *DB) nonBlockingResponseChan(ch chan error, err error) {
	select {
	case ch <- err:
	default:
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

