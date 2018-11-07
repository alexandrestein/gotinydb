package gotinydb

import (
	"github.com/dgraph-io/badger"
)

type (
	// Iterator provides a nice way to list elements
	Iterator struct {
		txn        *badger.Txn
		c          *Collection
		badgerIter *badger.Iterator
		item       *badger.Item
		colPrefix  []byte
	}
)

func (i *Iterator) get(dest interface{}) []byte {
	caller := new(multiGetCaller)
	caller.id = i.GetID()
	caller.dbID = i.getDBKey()
	caller.pointer = dest

	caller.encryptedAsBytes, _ = i.item.ValueCopy(caller.encryptedAsBytes)

	i.c.decryptAndUnmarshal(caller)

	return caller.asBytes
}

// GetBytes returns the document as a slice of bytes
func (i *Iterator) GetBytes() []byte {
	return i.get(nil)
}

// GetValue tries to fill-up the dest pointer with the coresponding document
func (i *Iterator) GetValue(dest interface{}) {
	i.get(dest)
}

func (i *Iterator) getDBKey() []byte {
	if !i.Valid() {
		return nil
	}

	tmpDBKey := i.item.Key()
	dbKey := make([]byte, len(tmpDBKey))
	copy(dbKey, tmpDBKey)

	return dbKey
}

// GetID returns the collection id if the current element
func (i *Iterator) GetID() string {
	dbKey := i.getDBKey()

	cleanDBKey := dbKey[len(i.colPrefix):]
	return string(cleanDBKey)
}

// Next moves the cursor to the next position. If the iterator is in regular mode
// it will move to the smallest bigger key than the current one. If the iterator is
// in reverted mode it will move to the biggest smaller key than the current one.
func (i *Iterator) Next() {
	i.badgerIter.Next()
}

// Valid returns true if the cursor still on valid value.
// It returns false if the iteration is done
func (i *Iterator) Valid() bool {
	valid := i.badgerIter.ValidForPrefix(i.colPrefix)
	if !valid {
		return false
	}

	i.item = i.badgerIter.Item()
	return !i.item.IsDeletedOrExpired()
}

// Seek would seek to the provided key if present.
// If absent, it would seek to the next smallest key greater than provided
// if iterating in the forward direction. Behavior would be reversed is iterating backwards.
func (i *Iterator) Seek(id string) {
	i.badgerIter.Seek(i.c.buildDBKey(id))
}

// Close closes the current iterator and it's related components.
// This method needs to be called ones the iterator is no more needed.
func (i *Iterator) Close() {
	i.badgerIter.Close()
	i.txn.Discard()
}
