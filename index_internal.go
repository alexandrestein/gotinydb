package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"reflect"
	"strconv"
	"time"

	"github.com/dgraph-io/badger"
)

func (i *indexType) getIDsForOneValue(ctx context.Context, indexedValue []byte) (ids *idsType, err error) {
	txn := i.getTx(false)
	defer txn.Discard()

	indexedValueID := i.getIDBuilder(indexedValue)

	asItem, err := txn.Get(indexedValueID)
	if err != nil {
		return nil, err
	}
	var asEncryptedBytes []byte
	asEncryptedBytes, err = asItem.Value()
	if err != nil {
		return nil, err
	}
	var asBytes []byte
	asBytes, err = decrypt(i.options.privateCryptoKey, asItem.Key(), asEncryptedBytes)
	if err != nil {
		return nil, err
	}

	ids, err = newIDs(ctx, i.selectorHash(), indexedValue, asBytes)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (i *indexType) getIDsForRangeOfValues(ctx context.Context, filterValue, limit []byte, increasing bool) (allIDs *idsType, err error) {
	txn := i.getTx(false)
	defer txn.Discard()

	// Initiate the iterator
	iterOptions := badger.DefaultIteratorOptions
	if !increasing {
		iterOptions.Reverse = true
	}
	iter := txn.NewIterator(iterOptions)
	defer iter.Close()

	indexedValueID := i.getIDBuilder(filterValue)

	// Go to the requested position and get the values of it
	iter.Seek(indexedValueID)
	if !iter.ValidForPrefix(i.getIDBuilder(nil)) {
		return nil, ErrNotFound
	}

	firstIndexedValueAsBytes := iter.Item().Key()
	firstIDsAsEncryptedBytes, err := iter.Item().Value()
	if err != nil {
		return nil, err
	}

	var firstIDsAsBytes []byte
	firstIDsAsBytes, err = decrypt(i.options.privateCryptoKey, iter.Item().Key(), firstIDsAsEncryptedBytes)
	if err != nil {
		return nil, err
	}

	firstIDsValue, unmarshalIDsErr := newIDs(ctx, i.selectorHash(), filterValue, firstIDsAsBytes)
	if unmarshalIDsErr != nil {
		return nil, unmarshalIDsErr
	}

	allIDs, _ = newIDs(ctx, i.selectorHash(), filterValue, nil)

	// If the index is not string index or if index is a string but the filter value is contained into the indexed value
	if i.Type != StringIndex || bytes.Contains(firstIndexedValueAsBytes, filterValue) && i.Type == StringIndex {
		// if the asked value is found
		if !reflect.DeepEqual(firstIndexedValueAsBytes, filterValue) {
			allIDs.AddIDs(firstIDsValue)
		}
	}
	return i.getIDsForRangeOfValuesLoop(ctx, allIDs, iter, filterValue, limit)
}

func (i *indexType) getIDsForRangeOfValuesLoop(ctx context.Context, allIDs *idsType, iter *badger.Iterator, filterValue, limit []byte) (*idsType, error) {
	prefix := i.getIDBuilder(nil)
	for {
		iter.Next()
		if !iter.ValidForPrefix(prefix) {
			break
		}
		indexedValuePlusPrefixes := iter.Item().Key()
		idsAsEncryptedBytes, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		var idsAsBytes []byte
		idsAsBytes, err = decrypt(i.options.privateCryptoKey, iter.Item().Key(), idsAsEncryptedBytes)
		if err != nil {
			return nil, err
		}

		if len(indexedValuePlusPrefixes) <= 0 && len(idsAsBytes) <= 0 {
			break
		}

		// The indexed value needs at least to containe the filter value
		if i.Type == StringIndex && !bytes.Contains(indexedValuePlusPrefixes, filterValue) {
			continue
		}

		ids, unmarshalIDsErr := newIDs(ctx, i.selectorHash(), indexedValuePlusPrefixes, idsAsBytes)
		if unmarshalIDsErr != nil {
			return nil, unmarshalIDsErr
		}

		if limit != nil {
			// if keepEqual {
			if bytes.Compare(append(prefix, limit...), indexedValuePlusPrefixes) < 0 {
				break
			}
		}

		allIDs.AddIDs(ids)

		// Clean if to big
		if len(allIDs.IDs) > i.options.InternalQueryLimit {
			allIDs.IDs = allIDs.IDs[:i.options.InternalQueryLimit]
			break
		}
	}

	return allIDs, nil
}

func (i *indexType) queryEqual(ctx context.Context, ids *idsType, filter *Filter) {
	tmpIDs, getErr := i.getIDsForOneValue(ctx, filter.values[0].Bytes())
	if getErr != nil {
		log.Printf("Index.runQuery Equal: %s\n", getErr.Error())
		return
	}

	for _, tmpID := range tmpIDs.IDs {
		tmpID.values[i.selectorHash()] = filter.values[0].Bytes()
	}

	ids.AddIDs(tmpIDs)
}

func (i *indexType) queryGreaterLess(ctx context.Context, ids *idsType, filter *Filter) {
	greater := true
	if filter.getType() == less {
		greater = false
	}

	tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), nil, greater)
	if getIdsErr != nil {
		log.Printf("Index.runQuery Greater, Less: %s\n", getIdsErr.Error())
		return
	}

	ids.AddIDs(tmpIDs)
}

func (i *indexType) queryBetween(ctx context.Context, ids *idsType, filter *Filter) {
	// Needs two values to make between
	if len(filter.values) < 2 {
		return
	}
	tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), filter.values[1].Bytes(), true)
	if getIdsErr != nil {
		log.Printf("Index.runQuery Between: %s\n", getIdsErr.Error())
		return
	}

	ids.AddIDs(tmpIDs)
}

func (i *indexType) queryExists(ctx context.Context, ids *idsType, filter *Filter) {
	txn := i.getTx(false)
	defer txn.Discard()

	prefixID := i.getIDBuilder(nil)

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(prefixID); iter.ValidForPrefix(prefixID); iter.Next() {
		asEncryptedBytes, err := iter.Item().Value()
		if err != nil {
			return
		}
		var asBytes []byte
		asBytes, err = decrypt(i.options.privateCryptoKey, iter.Item().Key(), asEncryptedBytes)
		if err != nil {
			return
		}

		var tmpIDs *idsType
		tmpIDs, err = newIDs(ctx, i.selectorHash(), iter.Item().Key()[len(prefixID):], asBytes)
		if err != nil {
			return
		}
		ids.AddIDs(tmpIDs)

		// Clean if to big
		if len(ids.IDs) >= i.options.InternalQueryLimit {
			ids.IDs = ids.IDs[:i.options.InternalQueryLimit]
			break
		}
	}

	return
}

func (i *indexType) queryContains(ctx context.Context, ids *idsType, filter *Filter) {
	if i.Type != StringIndex {
		return
	}

	txn := i.getTx(false)
	defer txn.Discard()

	prefixID := i.getIDBuilder(nil)

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(prefixID); iter.ValidForPrefix(prefixID); iter.Next() {
		asEncryptedBytes, err := iter.Item().Value()
		if err != nil {
			return
		}
		var asBytes []byte
		asBytes, err = decrypt(i.options.privateCryptoKey, iter.Item().Key(), asEncryptedBytes)
		if err != nil {
			return
		}

		var tmpIDs *idsType
		tmpIDs, err = newIDs(ctx, i.selectorHash(), iter.Item().Key()[len(prefixID):], asBytes)
		if err != nil {
			return
		}

		if len(tmpIDs.IDs) <= 0 {
			continue
		}

		// Check if the filter value as byte is inside the indexed value
		if bytes.Contains(iter.Item().Key()[len(prefixID):], filter.values[0].Bytes()) {
			// If yes all the related ids containe also the filter value
			ids.AddIDs(tmpIDs)
		}

		// Clean if to big
		if len(ids.IDs) >= i.options.InternalQueryLimit {
			ids.IDs = ids.IDs[:i.options.InternalQueryLimit]
			break
		}
	}

	return
}

// convertInterfaceValueFromMapToIndexType is used when indexing after insertion.
// A pointer needs to read the all collection to index values.
// But the values are not type any more and it needs to be recovered from the JSON record.
func (i *indexType) convertInterfaceValueFromMapToIndexType(input interface{}) (valueTypedForIndex interface{}) {
	switch typed := input.(type) {
	case string:
		switch i.Type {
		case StringIndex:
			return typed
		case TimeIndex:
			t, _ := time.Parse(time.RFC3339, typed)
			return t
		}
	case json.Number:
		switch i.Type {
		case IntIndex:
			asInt, _ := typed.Int64()
			return asInt
		case UIntIndex:
			asUint, _ := strconv.ParseUint(typed.String(), 10, 64)
			return uint64(asUint)
		}
	case []interface{}:
		ret := make([]interface{}, len(typed))
		for j := range typed {
			ret[j] = i.convertInterfaceValueFromMapToIndexType(typed[j])
		}
		return ret
	}
	return
}
