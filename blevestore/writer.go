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
	"fmt"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger"
)

type Writer struct {
	store      *Store
	operations []*transaction.Transaction
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.store.mo)
}

// func (w *Writer) add(dbID, content []byte) {
// 	elem := new(transactions.WriteElement)
// 	elem.DBKey = dbID
// 	elem.ContentAsBytes = content
// 	w.listOfWrites = append(w.listOfWrites, elem)

// 	// encrypted := cipher.Encrypt(w.store.config.key, dbID, content)
// 	// req := NewBleveStoreWriteRequest(dbID, encrypted)

// 	// w.store.config.bleveWriteChan <- req

// 	// err := <-req.ResponseChan
// 	// if err != nil {
// 	// 	fmt.Println("done", err)
// 	// }

// 	// return err
// }

func (w *Writer) write() error {
	for _, ope := range w.operations {
		// Sand to the write routine
		select {
		case w.store.config.writesChan <- ope:
		case <-w.store.config.ctx.Done():
			return w.store.config.ctx.Err()
		}

		// Wait for the response
		select {
		case err := <-ope.ResponseChan:
			if err != nil {
				return err
			}
		case <-ope.Ctx.Done():
			return ope.Ctx.Err()
		case <-w.store.config.ctx.Done():
			return w.store.config.ctx.Err()
		}
	}

	return nil
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) (err error) {
	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = w.store.config.db.View(func(txn *badger.Txn) (err error) {
		for k, mergeOps := range emulatedBatch.Merger.Merges {
			kb := []byte(k)

			storeID := w.store.buildID(kb)

			var item *badger.Item
			existingVal := []byte{}
			item, err = txn.Get(storeID)
			// If the KV pair exists the existing value is saved
			if err == nil {
				var encryptedValue []byte
				encryptedValue, err = item.ValueCopy(existingVal)
				if err != nil {
					return
				}

				existingVal, err = cipher.Decrypt(w.store.config.key, storeID, encryptedValue)
				if err != nil {
					return
				}
			}

			mergedVal, fullMergeOk := w.store.mo.FullMerge(kb, existingVal, mergeOps)
			if !fullMergeOk {
				err = fmt.Errorf("merge operator returned failure")
				return
			}

			// err = txn.Set(storeID, cipher.Encrypt(w.store.config.key, storeID, mergedVal))
			// w.writeTransaction.AddTransaction(
			// 	transactions.NewTransactionElement(storeID, mergedVal),
			// )
			w.operations = append(w.operations,
				transaction.NewTransaction(ctx, storeID, mergedVal, false),
			)
		}

		for _, op := range emulatedBatch.Ops {
			storeID := w.store.buildID(op.K)

			if op.V != nil {
				// err = txn.Set(storeID, cipher.Encrypt(w.store.config.key, storeID, op.V))
				// w.writeTransaction.AddTransaction(
				// 	transactions.NewTransactionElement(storeID, op.V),
				// )
				w.operations = append(w.operations,
					transaction.NewTransaction(ctx, storeID, op.V, false),
				)
			} else {
				// w.writeTransaction.AddTransaction(
				// 	transactions.NewTransactionElement(storeID, nil),
				// )
				w.operations = append(w.operations,
					transaction.NewTransaction(ctx, storeID, nil, true),
				)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return w.write()
}

func (w *Writer) Close() error {
	return nil
}
