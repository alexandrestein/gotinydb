//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blevestore

import (
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/dgraph-io/badger"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/test"
)

var (
	testKey        = [32]byte{}
	testDB         *badger.DB
	testWritesChan = make(chan *transaction.Transaction, 0)

	testPrefix = []byte{1, 9}

	testPath = os.TempDir() + "/blevestoreTest"
)

// var (
// 	key = [32]byte{}

// 	encryptFunc = func(dbID, clearContent []byte) (encryptedContent []byte) {
// 		return cipher.Encrypt(key, dbID, clearContent)
// 	}
// 	decryptFunc = func(dbID, encryptedContent []byte) (clearContent []byte, _ error) {
// 		return cipher.Decrypt(key, dbID, encryptedContent)
// 	}
// )

func init() {
	rand.Read(testKey[:])
}

func open(t *testing.T, testCtx context.Context, mo store.MergeOperator) store.KVStore {
	opt := badger.DefaultOptions
	opt.Dir = testPath
	opt.ValueDir = testPath

	var err error
	testDB, err = badger.Open(opt)
	if err != nil {
		t.Error(err)
		return nil
	}

	go goRoutineLoopForWrites(testCtx)

	var config *BleveStoreConfig
	config = NewBleveStoreConfig(testCtx, testKey, testPrefix, testDB, testWritesChan)

	var rv store.KVStore
	rv, err = New(mo, map[string]interface{}{
		"path":   "test",
		"config": config,
	})
	// rv, err = New(mo, map[string]interface{}{
	// 	"path":     "test",
	// 	"prefix":   []byte{1, 9},
	// 	"db":       db,
	// 	"writeTxn": writeTxn,
	// })
	if err != nil {
		t.Error(err)
	}

	return rv
}

func goRoutineLoopForWrites(testCtx context.Context) {
	// for {
	// 	var ops *transaction.Transaction
	// 	// select {
	// 	// case op, ok := <-writesChan:
	// 	// 	if !ok {
	// 	// 		return
	// 	// 	}
	// 	// 	ops = append(ops, op)
	// 	// }
	// 	select {
	// 	// There is an other request in the queue
	// 	case ops = <-testWritesChan:
	// 	case <-testCtx.Done():
	// 		return
	// 	}

	// 	err := testDB.Update(func(txn *badger.Txn) error {
	// 		var err error
	// 		if ops.Delete {
	// 			err = txn.Delete(ops.DBKey)
	// 		} else {
	// 			err = txn.Set(ops.DBKey, cipher.Encrypt(testKey, ops.DBKey, ops.Value))
	// 		}
	// 		if err != nil {
	// 			fmt.Println(err)
	// 		}
	// 		return nil
	// 	})

	// 	ops.ResponseChan <- err

	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// }

	for {
		var op *transaction.Transaction
		var ok bool
		select {
		case op, ok = <-testWritesChan:
			if !ok {
				return
			}
		case <-testCtx.Done():
			return
		}

		// Add to the list of operation to be done
		waitingWrites := []*transaction.Transaction{op}

		// Try to empty the queue if any
	tryToGetAnOtherRequest:
		select {
		// There is an other request in the queue
		case nextWrite := <-testWritesChan:
			// And save the response channel
			waitingWrites = append(waitingWrites, nextWrite)

			goto tryToGetAnOtherRequest
		case <-testCtx.Done():
			return
			// Stop waiting and do present operations
		default:
		}

		err := testDB.Update(func(txn *badger.Txn) error {
			for _, op := range waitingWrites {
				var err error
				if op.Delete {
					// fmt.Println("delete", op.DBKey)
					err = txn.Delete(op.DBKey)
				} else if op.CleanHistory {
					err = txn.SetWithDiscard(op.DBKey, cipher.Encrypt(testKey, op.DBKey, op.Value), 0)
				} else {
					// fmt.Println("write", op.DBKey)
					err = txn.Set(op.DBKey, cipher.Encrypt(testKey, op.DBKey, op.Value))
				}
				// Returns the write error to the caller
				if err != nil {
					go nonBlockingResponseChan(testCtx,op, err)
				}
			}
			return nil
		})

		// Dispatch the commit response to all callers
		for _, op := range waitingWrites {
			go nonBlockingResponseChan(testCtx,op, err)
		}
	}
}

func  nonBlockingResponseChan(testCtx context.Context, tx *transaction.Transaction, err error) {
	if tx.ResponseChan == nil {
		return
	}
	select {
	case tx.ResponseChan <- err:
	case <-testCtx.Done():
	case <-tx.Ctx.Done():
	}
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Error(err)
		return
	}

	err = testDB.Close()
	if err != nil {
		t.Error(err)
		return
	}

	err = os.RemoveAll(testPath)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestBadgerDBKVCrud(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestBadgerDBReaderIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestBadgerDBReaderOwnsGetBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestBadgerDBWriterOwnsBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestBadgerDBPrefixIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestBadgerDBPrefixIteratorSeek(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestBadgerDBRangeIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestBadgerDBRangeIteratorSeek(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

func TestBadgerDBMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(t, ctx, &test.TestMergeCounter{})
	defer cleanup(t, s)
	test.CommonTestMerge(t, s)
}

// func TestBadgerDBConfig(t *testing.T) {
// 	path := "test"
// 	defer os.RemoveAll(path)
// 	os.RemoveAll(path)

// 	opt := badger.DefaultOptions
// 	opt.Dir = path
// 	db, _ := badger.Open(opt)

// 	var writeTxn *badger.Txn

// 	var tests = []struct {
// 		in            map[string]interface{}
// 		name          string
// 		indexPrefixID []byte
// 		db            *badger.DB
// 	}{
// 		{
// 			map[string]interface{}{
// 				"path":     "test",
// 				"prefix":   []byte{1, 9},
// 				"db":       db,
// 				"key":      &[32]byte{},
// 				"writeTxn": writeTxn,
// 			},
// 			"test",
// 			[]byte{1, 9},
// 			db,
// 		},
// 		{
// 			map[string]interface{}{
// 				"path":     "test 2",
// 				"prefix":   []byte{2, 5},
// 				"key":      &[32]byte{},
// 				"db":       db,
// 				"writeTxn": writeTxn,
// 			},
// 			"test 2",
// 			[]byte{2, 5},
// 			db,
// 		},
// 	}

// 	for _, test := range tests {
// 		kv, err := New(nil, test.in)
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}
// 		bs, ok := kv.(*Store)
// 		if !ok {
// 			t.Error("failed type assertion to *boltdb.Store")
// 			return
// 		}
// 		if bs.name != test.name {
// 			t.Errorf("path: expected %q, got %q", test.name, bs.name)
// 			return
// 		}
// 		if !reflect.DeepEqual(bs.config.prefix, test.indexPrefixID) {
// 			t.Errorf("prefix: expected %X, got %X", test.indexPrefixID, bs.config.prefix)
// 			return
// 		}
// 		if bs.config.db != test.db {
// 			t.Errorf("db: expected %v, got %v", test.db, bs.config.db)
// 			return
// 		}
// 	}
// }
