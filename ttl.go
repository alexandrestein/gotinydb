package gotinydb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/dgraph-io/badger"
)

type (
	// TTL defines a type to permit to delete documents or files after a certain duration
	ttl struct {
		CleanTime              time.Time
		File                   bool
		DocumentCollectionName string
		DocumentID             string
	}
)

func (t *ttl) timeAsKey() []byte {
	prefix := []byte{prefixTTL}
	ret, _ := t.CleanTime.MarshalBinary()
	return append(prefix, ret...)
}
func (t *ttl) exportAsBytes() []byte {
	ret, _ := json.Marshal(t)
	return ret
}

func parseTTL(input []byte) (*ttl, error) {
	obj := new(ttl)
	err := json.Unmarshal(input, obj)
	return obj, err
}

func newTTL(colID, docOrFileID string, file bool, ttlDur time.Duration) *ttl {
	if ttlDur <= 0 {
		return nil
	}

	return &ttl{
		CleanTime:              time.Now().Add(ttlDur),
		File:                   file,
		DocumentCollectionName: colID,
		DocumentID:             docOrFileID,
	}
}

func (d *DB) goWatchForTTLToClean() {
start:
	var nextRun time.Duration

	ctx, cancel := context.WithTimeout(d.ctx, time.Second*10)
	defer cancel()
	tr := transaction.New(ctx)

	d.badger.View(func(txn *badger.Txn) (err error) {
		iterOp := badger.DefaultIteratorOptions
		iterOp.AllVersions = true
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := []byte{prefixTTL}
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			var key []byte
			key = item.KeyCopy(key)

			encryptedData := []byte{}
			encryptedData, err = item.ValueCopy(encryptedData)
			if err != nil {
				return err
			}

			var clearData []byte
			clearData, err = d.decryptData(key, encryptedData)
			if err != nil {
				return err
			}

			var ttl *ttl
			ttl, err = parseTTL(clearData)
			if err != nil {
				return err
			}

			// If the time is passed
			if ttl.CleanTime.Before(time.Now()) {
				// If this is not a file
				if !ttl.File {
					// Get the related collection
					col, _ := d.Use(ttl.DocumentCollectionName)
					// Pass if any error
					if col == nil {
						continue
					}

					// Tries to delete the document
					err = col.Delete(ttl.DocumentID)
				} else {
					err = d.FileStore.DeleteFile(ttl.DocumentID)
				}
				// If any error the TTL record is not remove to run the task again
				if err == nil {
					cleanTTLRecordOp := transaction.NewOperation("", "", ttl.timeAsKey(), nil, true, false)
					tr.AddOperation(cleanTTLRecordOp)
				}
			} else {
				// Setup the next run
				nextRun = ttl.CleanTime.Sub(time.Now())
				if nextRun > time.Second {
					nextRun = time.Second
				}
				return
			}
		}

		return nil
	})

	// DB closed
	if closed := d.ctx.Err(); closed != nil {
		return
	}

	// Do the writing:
	select {
	case d.writeChan <- tr:
	case <-d.ctx.Done():
		return
	}

	select {
	case <-tr.ResponseChan:
	case <-tr.Ctx.Done():
	}

	if nextRun == 0 {
		nextRun = time.Second
	}

	// Found but in some time.
	// Wait for it before stating the loop again.
	select {
	case <-time.After(nextRun):
		goto start
	case <-d.ctx.Done():
		return
	}
}
