package gotinydb

import (
	"encoding/json"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/dgraph-io/badger"
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

		db   *DB
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

// GetMeta returns the metadata of the actual cursor position
func (i *FileIterator) GetMeta() *FileMeta {
	return i.meta
}

// Next moves to the next valid metadata element
func (i *FileIterator) Next() error {
	i.badgerIter.Next()

goToNext:
	if !i.Valid() {
		return ErrFileItemIteratorNotValid
	}

	isMeta, err := i.isMetaChunk()
	if !isMeta || err != nil {
		if err != nil {
			return err
		}

		i.badgerIter.Next()
		goto goToNext
	}

	return nil
}

// Seek moves to the meta coresponding to the given id
func (i *FileIterator) Seek(id string) {
	i.badgerIter.Seek(i.db.buildFilePrefix(id, 0))
}

// Valid checks if the cursor point a valid metadata document
func (i *FileIterator) Valid() bool {
	valid := i.valid([]byte{prefixFiles})
	if valid {
		i.isMetaChunk()
	}
	return valid
}

func (i *FileIterator) isMetaChunk() (bool, error) {
	dbKey := i.item.Key()
	if len(dbKey) != 34 || dbKey[len(dbKey)-1] != 0 {
		return false, nil
	}

	buff, err := i.decrypt()
	if err != nil {
		return false, err
	}

	meta := new(FileMeta)
	err = json.Unmarshal(buff, meta)
	if err != nil {
		return false, err
	}

	i.meta = meta

	return true, nil
}

func (i *FileIterator) decrypt() ([]byte, error) {
	valAsEncryptedBytes, err := i.item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	var valAsBytes []byte
	valAsBytes, err = cipher.Decrypt(i.db.PrivateKey, i.item.Key(), valAsEncryptedBytes)
	if err != nil {
		return nil, err
	}

	return valAsBytes, nil
}
