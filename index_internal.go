package gotinydb

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/dgraph-io/badger"
)

func (i *indexType) getIDsForOneValue(ctx context.Context, indexedValue []byte) (ids *idsType, err error) {
	tx := i.getTx(false)
	defer tx.Discard()

	indexedValueID := append(i.getIDBuilder(nil), indexedValue...)
	asItem, err := tx.Get(indexedValueID)
	if err != nil {
		return nil, err
	}
	var asBytes []byte
	asBytes, err = asItem.Value()
	if err != nil {
		return nil, err
	}

	ids, err = newIDs(ctx, i.selectorHash(), indexedValue, asBytes)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (i *indexType) getIDsForRangeOfValues(ctx context.Context, filterValue, limit []byte, keepEqual, increasing bool) (allIDs *idsType, err error) {
	tx := i.getTx(false)
	defer tx.Discard()

	// Initiate the iterator
	iterOptions := badger.DefaultIteratorOptions
	if !increasing {
		iterOptions.Reverse = true
	}
	iter := tx.NewIterator(iterOptions)
	defer iter.Close()

	indexedValueID := append(i.getIDBuilder(nil), filterValue...)

	// Go to the requested position and get the values of it
	iter.Seek(indexedValueID)
	if !iter.ValidForPrefix(i.getIDBuilder(nil)) {
		fmt.Println("i.getIDBuilder(nil)", i.getIDBuilder(nil))
		return nil, ErrNotFound
	}

	firstIndexedValueAsByte := iter.Item().Key()
	firstIDsAsByte, err := iter.Item().Value()
	if err != nil {
		return nil, err
	}

	// firstIndexedValueAsByte, firstIDsAsByte := iter.Item().
	firstIDsValue, unmarshalIDsErr := newIDs(ctx, i.selectorHash(), filterValue, firstIDsAsByte)
	if unmarshalIDsErr != nil {
		fmt.Println(7, "key:", firstIndexedValueAsByte, "valAsString:", string(firstIDsAsByte))
		fmt.Println("ERRRRrrr", indexedValueID, unmarshalIDsErr, string(firstIDsAsByte))
		return nil, unmarshalIDsErr
	}

	allIDs, _ = newIDs(ctx, i.selectorHash(), filterValue, nil)

	// If the index is not string index or if index is a string but the filter value is contained into the indexed value
	if i.Type != StringIndex || bytes.Contains(firstIndexedValueAsByte, filterValue) && i.Type == StringIndex {
		// if the asked value is found
		if !reflect.DeepEqual(firstIndexedValueAsByte, filterValue) {
			if increasing {
				allIDs.AddIDs(firstIDsValue)
			}
		} else if keepEqual {
			allIDs.AddIDs(firstIDsValue)
		}
	}
	return i.getIDsForRangeOfValuesLoop(ctx, allIDs, iter, filterValue, limit, keepEqual)
}

func (i *indexType) getIDsForRangeOfValuesLoop(ctx context.Context, allIDs *idsType, iter *badger.Iterator, filterValue, limit []byte, keepEqual bool) (*idsType, error) {
	limitID := i.getIDBuilder(limit)
	for {
		iter.Next()
		if !iter.ValidForPrefix(limitID) {
			break
		}
		indexedValue := iter.Item().Key()
		idsAsByte, err := iter.Item().Value()
		if err != nil {
			return nil, err
		}
		if len(indexedValue) <= 0 && len(idsAsByte) <= 0 {
			break
		}

		// The indexed value needs at least to containe the filter value
		if i.Type == StringIndex && !bytes.Contains(indexedValue, filterValue) {
			continue
		}

		ids, unmarshalIDsErr := newIDs(ctx, i.selectorHash(), indexedValue, idsAsByte)
		if unmarshalIDsErr != nil {
			return nil, unmarshalIDsErr
		}

		if limit != nil {
			if keepEqual {
				if bytes.Compare(limit, indexedValue) < 0 {
					break
				}
			} else {
				if bytes.Compare(limit, indexedValue) <= 0 {
					break
				}
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

func (i *indexType) queryEqual(ctx context.Context, ids *idsType, filter Filter) {
	for _, value := range filter.getFilterBase().values {
		tmpIDs, getErr := i.getIDsForOneValue(ctx, value.Bytes())
		if getErr != nil {
			log.Printf("Index.runQuery Equal: %s\n", getErr.Error())
			return
		}

		for _, tmpID := range tmpIDs.IDs {
			tmpID.values[i.selectorHash()] = value.Bytes()
		}

		ids.AddIDs(tmpIDs)
	}
}

func (i *indexType) queryGreaterLess(ctx context.Context, ids *idsType, filter Filter) {
	greater := true
	if filter.getFilterBase().GetType() == Less {
		greater = false
	}

	tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.getFilterBase().values[0].Bytes(), nil, filter.getFilterBase().equal, greater)
	if getIdsErr != nil {
		log.Printf("Index.runQuery Greater, Less: %s\n", getIdsErr.Error())
		return
	}

	ids.AddIDs(tmpIDs)
}

func (i *indexType) queryBetween(ctx context.Context, ids *idsType, filter Filter) {
	// Needs two values to make between
	if len(filter.getFilterBase().values) < 2 {
		return
	}
	tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.getFilterBase().values[0].Bytes(), filter.getFilterBase().values[1].Bytes(), filter.getFilterBase().equal, true)
	if getIdsErr != nil {
		log.Printf("Index.runQuery Between: %s\n", getIdsErr.Error())
		return
	}

	ids.AddIDs(tmpIDs)
}
