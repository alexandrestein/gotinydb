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
	"fmt"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/dgraph-io/badger"
)

// Iterator is self explained
type Iterator struct {
	store    *Store
	iterator *badger.Iterator
	prefix   []byte
	start    []byte
	end      []byte
}

// Seek move to a specific location
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

// Next moves to the next value
func (i *Iterator) Next() {
	i.iterator.Next()
}

// Current returns the current stat of the pointer
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

// Key returns the key of the given position
func (i *Iterator) Key() []byte {
	if !i.Valid() {
		return nil
	}
	return i.key()
}

// Value returns the values for the actual position
func (i *Iterator) Value() []byte {
	if !i.Valid() {
		return nil
	}

	item := i.iterator.Item()

	var encryptVal []byte
	var err error
	encryptVal, err = item.ValueCopy(encryptVal)
	if err != nil {
		fmt.Println("err decrypt iterator blevestore 1", err, item.Key())
		return nil
	}

	val := []byte{}
	val, err = cipher.Decrypt(i.store.config.key, item.Key(), encryptVal)
	if err != nil {
		fmt.Println("err decrypt iterator blevestore 2", err, item.Key())
		return nil
	}

	return val
}

// Valid returns try if the value and the ID of the actual iterator position are both valid
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

// Close closes the iterator
func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
