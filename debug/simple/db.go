package simple

import (
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/debug/simple/transaction"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

type (
	DB struct {
		ConfigKey, PrivateKey [32]byte

		Path        string
		Badger      *badger.DB
		Collections []*Collection

		writeChan chan *transaction.Transaction
	}

	dbElement struct {
		Name string
		// Prefix defines the all prefix to the values
		Prefix []byte
	}
)

func New(path string, configKey [32]byte) (db *DB, err error) {
	db = new(DB)
	db.Path = path
	db.ConfigKey = configKey

	options := badger.DefaultOptions
	options.Dir = path
	options.ValueDir = path

	db.Badger, err = badger.Open(options)
	if err != nil {
		return nil, err
	}

	go db.goRoutineLoopForWrites()

	return db, nil
}

func (d *DB) Use(colName string) (col *Collection, err error) {
	for _, col = range d.Collections {
		if col.Name == colName {
			if col.db == nil {
				col.db = d
			}
			return
		}
	}

	col = NewCollection(colName)
	tmpHash := blake2b.Sum256([]byte(colName))
	col.Prefix = tmpHash[:]
	col.db = d

	return
}

func (d *DB) Close() error {
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
					err = txn.Delete(op.DBKey)
				} else {
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
