package gotinydb

import (
	"bytes"
	"context"
	"reflect"
)

func (i *Index) getIDsForOneValue(ctx context.Context, indexedValue []byte) (ids *IDs, err error) {
	tx, getTxErr := i.getTx(false)
	if getTxErr != nil {
		return nil, getTxErr
	}

	bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
	asBytes := bucket.Get(indexedValue)

	ids, err = NewIDs(ctx, i.SelectorHash, indexedValue, asBytes)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (i *Index) getIDsForRangeOfValues(ctx context.Context, indexedValue, limit []byte, keepEqual, increasing bool) (allIDs *IDs, err error) {
	tx, getTxErr := i.getTx(false)
	if getTxErr != nil {
		return nil, getTxErr
	}

	bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
	// Initiate the cursor (iterator)
	iter := bucket.Cursor()
	// Go to the requested position and get the values of it
	firstIndexedValueAsByte, firstIDsAsByte := iter.Seek(indexedValue)
	firstIDsValue, unmarshalIDsErr := NewIDs(ctx, i.SelectorHash, indexedValue, firstIDsAsByte)
	if unmarshalIDsErr != nil {
		return nil, unmarshalIDsErr
	}

	allIDs, _ = NewIDs(ctx, i.SelectorHash, indexedValue, nil)

	// if the asked value is found
	if reflect.DeepEqual(firstIndexedValueAsByte, indexedValue) && keepEqual {
		allIDs.AddIDs(firstIDsValue)
	}

	var nextFunc func() (key []byte, value []byte)
	if increasing {
		nextFunc = iter.Next
	} else {
		nextFunc = iter.Prev
	}

	for {
		indexedValue, idsAsByte := nextFunc()
		if len(indexedValue) <= 0 && len(idsAsByte) <= 0 {
			break
		}
		ids, unmarshalIDsErr := NewIDs(ctx, i.SelectorHash, indexedValue, idsAsByte)
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
		if len(allIDs.IDs) > i.conf.InternalQueryLimit {
			allIDs.IDs = allIDs.IDs[:i.conf.InternalQueryLimit]
			break
		}
	}
	return allIDs, nil
}
