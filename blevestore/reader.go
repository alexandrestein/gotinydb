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
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger/v2"
)

// Reader implement the reader interface
type Reader struct {
	store     *Store
	txn       *badger.Txn
	iterators []*badger.Iterator
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

	var clear []byte
	clear, err = cipher.Decrypt(r.store.config.key, storeKey, rv)

	return clear, err
}

// Get returns the content of the given ID or an error if any
func (r *Reader) Get(key []byte) (ret []byte, err error) {
	return r.get(key)
}

// MultiGet returns multiple values of the given ID in on shot.
// It returns an error if any
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
	r.iterators = append(r.iterators, iter)

	rv := &Iterator{
		store:    r.store,
		iterator: iter,
	}

	return rv
}

// PrefixIterator builds a new iterator with the provided prefix
func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	rv := r.iterator()
	rv.prefix = prefix

	rv.Seek(prefix)
	return rv
}

// RangeIterator builds a new iterator which will start at start and automatically stop at end
func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := r.iterator()
	rv.start = start
	rv.end = end

	rv.Seek(start)
	return rv
}

// Close closes the reader
func (r *Reader) Close() error {
	for _, iter := range r.iterators {
		iter.Close()
	}

	r.txn.Discard()
	return nil
}
