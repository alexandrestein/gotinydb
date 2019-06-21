package gotinydb

import (
	"github.com/dgraph-io/badger/v2"
)

type (
	baseIterator struct {
		txn        *badger.Txn
		badgerIter *badger.Iterator
		item       *badger.Item
	}

	// CollectionIterator provides a nice way to list elements
	CollectionIterator struct {
		*baseIterator

		c         *Collection
		colPrefix []byte
	}

	// FileIterator provides easy access to all written files
	FileIterator struct {
		*baseIterator

		fs   *FileStore
		meta *FileMeta
	}
)

func (i *baseIterator) valid(prefix []byte) bool {
	valid := i.badgerIter.ValidForPrefix(prefix)
	if !valid {
		return false
	}

	item := i.badgerIter.Item()
	if item.IsDeletedOrExpired() {
		return false
	}

	i.item = item
	return true
}

// Close closes the current iterator and it's related components.
// This method needs to be called ones the iterator is no more needed.
func (i *baseIterator) Close() {
	i.badgerIter.Close()
	i.txn.Discard()
}

func (i *CollectionIterator) get(dest interface{}) []byte {
	caller := new(multiGetCaller)
	caller.id = i.GetID()
	caller.dbID = i.getDBKey()
	caller.pointer = dest

	caller.encryptedAsBytes, _ = i.item.ValueCopy(caller.encryptedAsBytes)

	i.c.decryptAndUnmarshal(caller)

	return caller.asBytes
}

// GetBytes returns the document as a slice of bytes
func (i *CollectionIterator) GetBytes() []byte {
	return i.get(nil)
}

// GetValue tries to fill-up the dest pointer with the coresponding document
func (i *CollectionIterator) GetValue(dest interface{}) {
	i.get(dest)
}

func (i *CollectionIterator) getDBKey() []byte {
	if !i.Valid() {
		return nil
	}

	tmpDBKey := i.item.Key()
	dbKey := make([]byte, len(tmpDBKey))
	copy(dbKey, tmpDBKey)

	return dbKey
}

// GetID returns the collection id if the current element
func (i *CollectionIterator) GetID() string {
	dbKey := i.getDBKey()
	if dbKey == nil {
		return ""
	}

	cleanDBKey := dbKey[len(i.colPrefix):]
	return string(cleanDBKey)
}

// Next moves the cursor to the next position. If the iterator is in regular mode
// it will move to the smallest bigger key than the current one. If the iterator is
// in reverted mode it will move to the biggest smaller key than the current one.
func (i *CollectionIterator) Next() {
	i.badgerIter.Next()
}

// Valid returns true if the cursor still on valid value.
// It returns false if the iteration is done
func (i *CollectionIterator) Valid() bool {
	return i.valid(i.colPrefix)
}

// Seek would seek to the provided key if present.
// If absent, it would seek to the next smallest key greater than provided
// if iterating in the forward direction. Behavior would be reversed is iterating backwards.
func (i *CollectionIterator) Seek(id string) {
	i.badgerIter.Seek(i.c.buildDBKey(id))
}
