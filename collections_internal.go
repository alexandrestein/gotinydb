package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
)

func (c *Collection) getCollectionPrefix() []byte {
	return []byte{prefixCollections, c.prefix}
}
func (c *Collection) buildCollectionPrefix(nextPrefix byte) []byte {
	return append(c.getCollectionPrefix(), nextPrefix)
}
func (c *Collection) buildIDWhitPrefixData(id []byte) []byte {
	ret := c.buildCollectionPrefix(prefixData)
	return append(ret, id...)
}

func (c *Collection) buildIDWhitPrefixBleveIndex(indexName, id []byte) []byte {
	ret := append(c.buildCollectionPrefix(prefixBleveIndexes), deriveName(indexName, 8)...)
	return append(ret, id...)
}

func (c *Collection) buildStoreID(id string) []byte {
	return c.buildIDWhitPrefixData([]byte(id))
}

func (c *Collection) buildIndexPath(name string) string {
	colHash := blake2b.Sum256([]byte(c.name))
	nameHash := blake2b.Sum256([]byte(name))

	return fmt.Sprintf("%s/%x/%x", c.options.Path, colHash[:8], nameHash[:8])
}

func (c *Collection) buildKvConfig(path string, indexPrefix []byte) (config map[string]interface{}) {
	return map[string]interface{}{
		"path": path,
		"config": blevestore.NewBleveStoreConfig(
			c.options.privateCryptoKey,
			indexPrefix,
			c.store,
			c.writeTransactionChan,
			c.options.TransactionTimeOut,
		),
	}
}

func (c *Collection) getInterfaceAsBytes(input interface{}) (contentAsBytes []byte, err error) {
	if tryBytes, ok := input.([]byte); ok {
		contentAsBytes = tryBytes
	} else {
		contentAsBytes, err = json.Marshal(input)
		if err != nil {
			return
		}
	}

	return
}

func (c *Collection) get(ctx context.Context, ids ...string) ([][]byte, error) {
	ret := make([][]byte, len(ids))
	return ret, c.store.View(func(txn *badger.Txn) error {
		for i, id := range ids {
			idAsBytes := c.buildStoreID(id)
			var err error
			var item *badger.Item
			item, err = txn.Get(idAsBytes)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return ErrNotFound
				}
				return err
			}

			if item.IsDeletedOrExpired() {
				return ErrNotFound
			}

			var contentAsEncryptedBytes []byte
			contentAsEncryptedBytes, err = item.ValueCopy(contentAsEncryptedBytes)
			if err != nil {
				return err
			}

			var contentAsBytes []byte
			contentAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, item.Key(), contentAsEncryptedBytes)
			if err != nil {
				return err
			}

			ret[i] = contentAsBytes
		}
		return nil
	})
}

// getStoredIDs returns all ids if it does not exceed the limit.
// This will not returned the ID used to set the value inside the collection
// It returns the id used to set the value inside the store
func (c *Collection) getStoredIDsAndValues(starter string, limit int, IDsOnly bool) ([]*Response, error) {
	response := make([]*Response, limit)

	return response, c.store.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		prefix := c.buildIDWhitPrefixData(nil)
		iter.Seek(c.buildIDWhitPrefixData([]byte(starter)))

		count := 0
		for ; iter.Valid(); iter.Next() {
			if !iter.ValidForPrefix(prefix) || count > limit-1 {
				response = response[:count]
				return nil
			}

			responseItem := new(Response)

			item := iter.Item()

			if item.IsDeletedOrExpired() {
				continue
			}

			responseItem.ID = string(item.Key()[len(c.buildIDWhitPrefixData(nil)):])

			if !IDsOnly {
				var err error
				responseItem.ContentAsBytes, err = item.ValueCopy(responseItem.ContentAsBytes)
				if err != nil {
					return err
				}

				responseItem.ContentAsBytes, err = cipher.Decrypt(c.options.privateCryptoKey, item.Key(), responseItem.ContentAsBytes)
				if err != nil {
					return err
				}
			}

			response[count] = responseItem

			count++
		}

		// Clean the end of the slice if not full
		response = response[:count]
		return nil
	})
}

func (c *Collection) isRunning() bool {
	if c.ctx.Err() != nil {
		return false
	}

	return true
}

func (c *Collection) deleteFromIndexes(id string) error {
	for _, i := range c.bleveIndexes {
		err := i.index.Delete(id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Collection) getBleveIndex(name string) (*bleveIndex, error) {
	var index *bleveIndex

	// Loop all indexes to found the given index
	found := false
	for _, i := range c.bleveIndexes {
		if i.Name == name {
			index = i
			found = true
			// If index is already loaded
			if index.index != nil {
				// oldBleveIndex := index.index
				// defer oldBleveIndex.Close()
				// index.index = nil
				// fmt.Println("Clean bleve")
				return index, nil
			}
			break
		}
	}

	if !found {
		return nil, ErrIndexNotFound
	}

	fmt.Println("1")
	index.KvConfig = c.buildKvConfig(index.Path, index.IndexPrefix)
	fmt.Println("2")
	// Load the index
	bleveIndex, err := bleve.OpenUsing(index.Path, index.KvConfig)
	fmt.Println("3")
	if err != nil {
		return nil, err
	}

	// Save the index interface into the internal index type
	index.index = bleveIndex

	fmt.Println("new loaded")
	for _, i := range c.bleveIndexes {
		if i.Name == name {
			fmt.Println(reflect.DeepEqual(i, index))
		}
	}

	return index, nil
}
