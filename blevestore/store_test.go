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
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transactions"
	"github.com/dgraph-io/badger"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/test"
)

var (
	key        = [32]byte{}
	db         *badger.DB
	writesChan = make(chan *transactions.WriteTransaction, 0)

	prefix = []byte{1, 9}
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
	rand.Read(key[:])

	go goRoutineLoopForWrites()
}

func open(t *testing.T, mo store.MergeOperator) store.KVStore {
	opt := badger.DefaultOptions
	opt.Dir = "test"
	opt.ValueDir = "test"

	var err error
	db, err = badger.Open(opt)
	if err != nil {
		t.Error(err)
		return nil
	}

	var config *BleveStoreConfig
	config = NewBleveStoreConfig(key, prefix, db, writesChan, time.Second*60)

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

func goRoutineLoopForWrites() {
	for {
		ops, ok := <-writesChan
		if !ok {
			return
		}

		err := db.Update(func(txn *badger.Txn) error {
			for _, op := range ops.Transactions {
				var err error
				if op.ContentAsBytes == nil {
					// if op.ContentAsBytes == nil || len(op.ContentAsBytes) == 0 {
					err = txn.Delete(op.DBKey)
				} else {
					err = txn.Set(op.DBKey, cipher.Encrypt(key, op.DBKey, op.ContentAsBytes))
				}
				if err != nil {
					fmt.Println(err)
				}
			}
			return nil
		})
		ops.ResponseChan <- err

		if err != nil {
			fmt.Println(err)
		}
	}
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Error(err)
		return
	}
	err = os.RemoveAll("test")
	if err != nil {
		t.Error(err)
		return
	}
}

func TestBadgerDBKVCrud(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestBadgerDBReaderIsolation(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestBadgerDBReaderOwnsGetBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestBadgerDBWriterOwnsBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestBadgerDBPrefixIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestBadgerDBPrefixIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestBadgerDBRangeIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestBadgerDBRangeIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

func TestBadgerDBMerge(t *testing.T) {
	s := open(t, &test.TestMergeCounter{})
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
