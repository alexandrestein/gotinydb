package blevestore

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

import (
	"fmt"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger"
)

type Reader struct {
	store         *Store
	txn           *badger.Txn
	indexPrefixID []byte
	iterators     []*badger.Iterator
}

func (r *Reader) get(key []byte) ([]byte, error) {
	storeKey := r.store.buildID(key)
	item, err := r.txn.Get(storeKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}

	var rv []byte
	rv, err = item.ValueCopy(rv)
	if err != nil {
		return nil, err
	}

	clear, err2 := cipher.Decrypt(r.store.config.key, storeKey, rv)
	if err2 != nil {
		fmt.Println("clear, err2", clear, err2, rv, key, len(rv))
	} else {
		// fmt.Println("OK", key)
	}
	return clear, err2
	// return r.store.decrypt(storeKey, rv)
}
func (r *Reader) Get(key []byte) (ret []byte, err error) {
	return r.get(key)
}

func (r *Reader) MultiGet(keys [][]byte) (rvs [][]byte, err error) {
	rvs = make([][]byte, len(keys))

	for i, key := range keys {
		rvs[i], err = r.get(key)
		if err != nil {
			return nil, err
		}
	}

	return rvs, nil
}

func (r *Reader) iterator() *Iterator {
	iter := r.txn.NewIterator(badger.DefaultIteratorOptions)

	rv := &Iterator{
		store:    r.store,
		iterator: iter,
	}

	r.iterators = append(r.iterators, iter)

	return rv
}

func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	rv := r.iterator()
	rv.prefix = prefix

	rv.Seek(r.store.buildID(prefix))
	return rv
}

func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := r.iterator()
	rv.start = start
	rv.end = end

	rv.Seek(r.store.buildID(start))
	return rv
}

func (r *Reader) Close() error {
	for _, iter := range r.iterators {
		iter.Close()
	}

	r.txn.Discard()
	return nil
}
