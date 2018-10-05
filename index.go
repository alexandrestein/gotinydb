package gotinydb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/dgraph-io/badger"
)

func (i *bleveIndex) indexAllValues(c *Collection) {
	collectionPrefix := c.buildIDWhitPrefixData(nil)
	c.store.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(collectionPrefix); iter.ValidForPrefix(collectionPrefix); iter.Next() {
			item := iter.Item()

			var err error
			var itemAsEncryptedBytes []byte
			itemAsEncryptedBytes, err = item.ValueCopy(itemAsEncryptedBytes)
			if err != nil {
				continue
			}

			var clearBytes []byte
			clearBytes, err = cipher.Decrypt(c.options.privateCryptoKey, item.Key(), itemAsEncryptedBytes)

			contentToIndex := i.fromValueBytesGetContentToIndex(clearBytes)
			if contentToIndex == nil {
				continue
			}

			id := string(item.Key()[len(collectionPrefix):])
			i.index.Index(id, contentToIndex)
		}

		return nil
	})
}

func (i *bleveIndex) fromValueBytesGetContentToIndex(input []byte) interface{} {
	var elem interface{}
	decoder := json.NewDecoder(bytes.NewBuffer(input))

	if jsonErr := decoder.Decode(&elem); jsonErr != nil {
		fmt.Println("errjsonErr", jsonErr)
		return nil
	}

	var ret interface{}
	switch typed := elem.(type) {
	case map[string]interface{}:
		ret = typed
	default:
		fmt.Println("bad reconstruction of the objects", reflect.TypeOf(elem), elem)
	}

	contentToIndex, apply := i.Selector.Apply(ret)
	if apply {
		return contentToIndex
	}

	return nil
}
