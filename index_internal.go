package gotinydb

import (
	"bytes"
	"context"
	"log"
	"reflect"
)

func (i *indexType) getIDsForOneValue(ctx context.Context, indexedValue []byte) (ids *idsType, err error) {
	tx, getTxErr := i.getTx(false)
	if getTxErr != nil {
		return nil, getTxErr
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
	asBytes := bucket.Get(indexedValue)

	ids, err = newIDs(ctx, i.SelectorHash, indexedValue, asBytes)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (i *indexType) getIDsForRangeOfValues(ctx context.Context, filterValue, limit []byte, keepEqual, increasing bool) (allIDs *idsType, err error) {
	tx, getTxErr := i.getTx(false)
	if getTxErr != nil {
		return nil, getTxErr
	}
	defer tx.Rollback()

	bucket := tx.Bucket([]byte("indexes")).Bucket([]byte(i.Name))
	// Initiate the cursor (iterator)
	iter := bucket.Cursor()
	// Go to the requested position and get the values of it
	firstIndexedValueAsByte, firstIDsAsByte := iter.Seek(filterValue)
	firstIDsValue, unmarshalIDsErr := newIDs(ctx, i.SelectorHash, filterValue, firstIDsAsByte)
	if unmarshalIDsErr != nil {
		return nil, unmarshalIDsErr
	}

	allIDs, _ = newIDs(ctx, i.SelectorHash, filterValue, nil)

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

		// The indexed value needs at least to containe the filter value
		if i.Type == StringIndex && !bytes.Contains(indexedValue, filterValue) {
			continue
		}

		ids, unmarshalIDsErr := newIDs(ctx, i.SelectorHash, indexedValue, idsAsByte)
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

func (i *indexType) queryEqual(ctx context.Context, ids *idsType, filter *Filter) {
	for _, value := range filter.values {
		tmpIDs, getErr := i.getIDsForOneValue(ctx, value.Bytes())
		if getErr != nil {
			log.Printf("Index.runQuery Equal: %s\n", getErr.Error())
			return
		}

		for _, tmpID := range tmpIDs.IDs {
			tmpID.values[i.SelectorHash] = value.Bytes()

		}

		ids.AddIDs(tmpIDs)
	}
}

func (i *indexType) queryGreaterLess(ctx context.Context, ids *idsType, filter *Filter) {
	greater := true
	if filter.GetType() == Less {
		greater = false
	}

	tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), nil, filter.equal, greater)
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
	tmpIDs, getIdsErr := i.getIDsForRangeOfValues(ctx, filter.values[0].Bytes(), filter.values[1].Bytes(), filter.equal, true)
	if getIdsErr != nil {
		log.Printf("Index.runQuery Between: %s\n", getIdsErr.Error())
		return
	}

	ids.AddIDs(tmpIDs)
}
