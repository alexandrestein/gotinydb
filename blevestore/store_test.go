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

func init() {
	rand.Read(testKey[:])
}

func open(testCtx context.Context, t *testing.T, mo store.MergeOperator) store.KVStore {
	opt := badger.DefaultOptions(testPath)

	var err error
	testDB, err = badger.Open(opt)
	if err != nil {
		t.Error(err)
		return nil
	}

	go goRoutineLoopForWrites(testCtx)

	var config *Config
	config = NewConfig(testCtx, testKey, testPrefix, testDB, testWritesChan)

	var rv store.KVStore
	rv, err = New(mo, map[string]interface{}{
		"path":   "test",
		"config": config,
	})
	if err != nil {
		t.Error(err)
	}

	return rv
}

func goRoutineLoopForWrites(testCtx context.Context) {
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
			for _, transaction := range waitingWrites {
				var err error
				for _, op := range transaction.Operations {
					if op.Delete {
						err = txn.Delete(op.DBKey)
					} else if op.CleanHistory {
						entry := badger.NewEntry(op.DBKey, cipher.Encrypt(testKey, op.DBKey, op.Value))
						entry.WithDiscard()
						err = txn.SetEntry(entry)
					} else {
						err = txn.Set(op.DBKey, cipher.Encrypt(testKey, op.DBKey, op.Value))
					}
					// Returns the write error to the caller
					if err != nil {
						go nonBlockingResponseChan(testCtx, transaction, err)
					}
				}
			}
			return nil
		})

		// Dispatch the commit response to all callers
		for _, op := range waitingWrites {
			go nonBlockingResponseChan(testCtx, op, err)
		}
	}
}

func nonBlockingResponseChan(testCtx context.Context, tx *transaction.Transaction, err error) {
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

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestBadgerDBReaderIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestBadgerDBReaderOwnsGetBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestBadgerDBWriterOwnsBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestBadgerDBPrefixIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestBadgerDBPrefixIteratorSeek(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestBadgerDBRangeIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestBadgerDBRangeIteratorSeek(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

func TestBadgerDBMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := open(ctx, t, &test.TestMergeCounter{})
	defer cleanup(t, s)
	test.CommonTestMerge(t, s)
}
