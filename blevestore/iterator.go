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
	"bytes"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/dgraph-io/badger"
)

type Iterator struct {
	store    *Store
	iterator *badger.Iterator
	prefix   []byte
	start    []byte
	end      []byte
}

func (i *Iterator) Seek(k []byte) {
	if i.start != nil && bytes.Compare(k, i.start) < 0 {
		k = i.start
	}
	if i.prefix != nil && !bytes.HasPrefix(k, i.prefix) {
		if bytes.Compare(k, i.prefix) < 0 {
			k = i.prefix
		}
	}

	i.iterator.Seek(i.store.buildID(k))
}

func (i *Iterator) Next() {
	i.iterator.Next()
}

func (i *Iterator) Current() (key []byte, val []byte, valid bool) {
	valid = i.Valid()
	if !valid {
		return
	}

	key = i.Key()
	val = i.Value()

	return
}

func (i *Iterator) key() (key []byte) {
	key = []byte{}
	key = i.iterator.Item().KeyCopy(key)
	key = key[len(i.store.config.prefix):]

	return
}

func (i *Iterator) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.key()
}

func (i *Iterator) Value() []byte {
	if !i.Valid() {
		return nil
	}

	item := i.iterator.Item()

	var encryptVal []byte
	encryptVal, _ = item.ValueCopy(encryptVal)

	val := []byte{}
	val, _ = cipher.Decrypt(i.store.config.key, item.Key(), encryptVal)
	// var err error
	// val, err = cipher.Decrypt(i.store.config.key, item.Key(), encryptVal)
	// fmt.Println("err", err, item.Key())

	return val
}

func (i *Iterator) Valid() bool {
	if !i.iterator.Valid() {
		return false
	}
	if i.prefix != nil {
		return i.iterator.ValidForPrefix(i.store.buildID(i.prefix))
		// If no prefix but end.
		// It's needed to check if the prefix still valid.
	} else if i.end != nil && i.iterator.ValidForPrefix(i.store.buildID(nil)) {
		return bytes.Compare(i.key(), i.end) < 0
	}
	return i.iterator.ValidForPrefix(i.store.buildID(nil))
}

func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
