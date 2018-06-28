package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/boltdb/bolt"
	"github.com/google/btree"
)

func (c *Collection) loadInfos() error {
	return c.DB.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("config"))
		if bucket == nil {
			return vars.ErrNotFound
		}

		name := string(bucket.Get([]byte("name")))
		id := string(bucket.Get([]byte("id")))
		c.Name = name
		c.ID = string(id)

		return nil
	})
}

func (c *Collection) init(name string) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		bucketsToCreate := []string{"config", "indexes", "refs"}
		for _, bucketName := range bucketsToCreate {
			if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
				return err
			}
		}

		confBucket := tx.Bucket([]byte("config"))
		if confBucket == nil {
			return fmt.Errorf("bucket does not exist")
		}
		if nameErr := confBucket.Put([]byte("name"), []byte(name)); nameErr != nil {
			return nameErr
		}
		if idErr := confBucket.Put([]byte("id"), []byte(c.ID)); idErr != nil {
			return idErr
		}
		return nil
	})
}

func (c *Collection) getIndexesFromConfigBucket() []*Index {
	indexes := []*Index{}
	c.DB.View(func(tx *bolt.Tx) error {
		indexesAsBytes := tx.Bucket([]byte("config")).Get([]byte("indexesList"))
		json.Unmarshal(indexesAsBytes, &indexes)

		return nil
	})
	return indexes
}

func (c *Collection) setIndexesIntoConfigBucket(index *Index) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		confBucket := tx.Bucket([]byte("config"))
		indexesAsBytes := confBucket.Get([]byte("indexesList"))
		indexes := []*Index{}
		json.Unmarshal(indexesAsBytes, &indexes)

		found := false
		for i, tmpIndex := range indexes {
			if tmpIndex.Name == index.Name {
				indexes[i] = index
				found = true
				break
			}
		}
		if !found {
			indexes = append(indexes, index)
		}

		indexesAsBytes, _ = json.Marshal(indexes)
		return confBucket.Put([]byte("indexesList"), indexesAsBytes)
	})
}
func (c *Collection) delIndexesIntoConfigBucket(indexName string) error {
	return c.DB.Update(func(tx *bolt.Tx) error {
		confBucket := tx.Bucket([]byte("config"))
		indexesAsBytes := confBucket.Get([]byte("indexesList"))
		indexes := []*Index{}
		err := json.Unmarshal(indexesAsBytes, &indexes)
		if err != nil {
			return err
		}

		for i, index := range indexes {
			if index.Name == indexName {
				copy(indexes[i:], indexes[i+1:])
				indexes[len(indexes)-1] = nil
				indexes = indexes[:len(indexes)-1]
				break
			}
		}

		indexesAsBytes, _ = json.Marshal(indexes)
		return confBucket.Put([]byte("indexesList"), indexesAsBytes)
	})
}

func (c *Collection) initWriteTransactionChan(ctx context.Context) {
	c.writeTransactionChan = make(chan *writeTransaction, 1000)
	go func() {
		for {
			select {
			case tr := <-c.writeTransactionChan:
				c.putTransaction(tr)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Collection) putTransaction(tr *writeTransaction) {
	storeDoneChannel := make(chan bool, 1)
	indexDoneChannel := make(chan bool, 1)
	storeErrChan := make(chan error, 1)
	indexErrChan := make(chan error, 1)

	go c.putIntoStore(tr.ctx, storeErrChan, storeDoneChannel, tr)

	if !tr.bin {
		go c.putIntoIndexes(tr.ctx, indexErrChan, indexDoneChannel, tr)
	} else {
		go c.onlyCleanRefs(tr.ctx, indexErrChan, indexDoneChannel, tr)
	}

	propagateFunc := func(ok bool, err error) {
		storeDoneChannel <- ok
		indexDoneChannel <- ok
		<-storeDoneChannel
		<-indexDoneChannel
		tr.responseChan <- err
		return
	}

waitNext:
	select {
	case err := <-storeErrChan:
		if err == nil {
			storeErrChan = nil
			if storeErrChan == nil && indexErrChan == nil {
				propagateFunc(true, err)
				break
			}
			goto waitNext
		}
		propagateFunc(false, err)
	case err := <-indexErrChan:
		if err == nil {
			indexErrChan = nil
			if storeErrChan == nil && indexErrChan == nil {
				propagateFunc(true, err)
				break
			}
			goto waitNext
		}
		propagateFunc(false, err)
	case <-tr.ctx.Done():
		propagateFunc(false, tr.ctx.Err())
	}
}

func (c *Collection) buildStoreID(id string) []byte {
	compositeID := fmt.Sprintf("%s_%s", c.Name, id)
	objectID := vars.BuildID(compositeID)
	return []byte(fmt.Sprintf("%s_%s", c.ID[:4], objectID))
}

func (c *Collection) putIntoIndexes(ctx context.Context, errChan chan error, doneChan chan bool, writeTransaction *writeTransaction) error {
	defer func() { doneChan <- true }()
	return c.DB.Update(func(tx *bolt.Tx) error {
		err := c.cleanRefs(ctx, tx, writeTransaction.id)
		if err != nil {
			return err
		}

		refsBucket := tx.Bucket([]byte("refs"))
		refsAsBytes := refsBucket.Get(vars.BuildBytesID(writeTransaction.id))
		refs := newRefs()
		if refsAsBytes != nil && len(refsAsBytes) > 0 {
			if err := json.Unmarshal(refsAsBytes, refs); err != nil {
				errChan <- err
				return err
			}
		}

		if refs.ObjectID == "" {
			refs.ObjectID = writeTransaction.id
		}
		if refs.ObjectHashID == "" {
			refs.ObjectHashID = vars.BuildID(writeTransaction.id)
		}

		for _, index := range c.Indexes {
			if indexedValue, apply := index.Apply(writeTransaction.contentInterface); apply {
				indexBucket := tx.Bucket([]byte("indexes")).Bucket([]byte(index.Name))

				idsAsBytes := indexBucket.Get(indexedValue)
				ids, parseIDsErr := newIDs(ctx, 0, nil, idsAsBytes)
				if parseIDsErr != nil {
					errChan <- parseIDsErr
					return parseIDsErr
				}

				id := newID(ctx, writeTransaction.id)
				ids.AddID(id)
				idsAsBytes = ids.MustMarshal()

				if err := indexBucket.Put(indexedValue, idsAsBytes); err != nil {
					errChan <- err
					return err
				}

				refs.setIndexedValue(index.Name, index.SelectorHash, indexedValue)
			}
		}

		putErr := refsBucket.Put(refs.IDasBytes(), refs.asBytes())
		if putErr != nil {
			errChan <- err
			return err
		}

		return c.endOfIndexUpdate(ctx, errChan, doneChan, writeTransaction)
	})
}

func (c *Collection) onlyCleanRefs(ctx context.Context, errChan chan error, doneChan chan bool, writeTransaction *writeTransaction) error {
	defer func() { doneChan <- true }()
	return c.DB.Update(func(tx *bolt.Tx) error {
		err := c.cleanRefs(ctx, tx, writeTransaction.id)
		if err != nil {
			errChan <- err
			return err
		}

		return c.endOfIndexUpdate(ctx, errChan, doneChan, writeTransaction)
	})
}

func (c *Collection) endOfIndexUpdate(ctx context.Context, errChan chan error, doneChan chan bool, writeTransaction *writeTransaction) error {
	errChan <- nil

	select {
	case ok := <-doneChan:
		if ok {
			return nil
		}
		return fmt.Errorf("error from outsid of the index")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Collection) cleanRefs(ctx context.Context, tx *bolt.Tx, idAsString string) error {
	indexBucket := tx.Bucket([]byte("indexes"))
	refsBucket := tx.Bucket([]byte("refs"))

	// Get the references of the given ID
	refsAsBytes := refsBucket.Get(vars.BuildBytesID(idAsString))
	refs := newRefs()
	if refsAsBytes != nil && len(refsAsBytes) > 0 {
		if err := json.Unmarshal(refsAsBytes, refs); err != nil {
			return err
		}
	}

	// Clean every reference of the object In all indexes if present
	for _, ref := range refs.Refs {
		for _, index := range c.Indexes {
			if index.Name == ref.IndexName {
				// If reference present in this index the reference is cleaned
				ids, newIDErr := newIDs(ctx, 0, nil, indexBucket.Bucket([]byte(index.Name)).Get(ref.IndexedValue))
				if newIDErr != nil {
					return newIDErr
				}
				ids.RmID(idAsString)
				// And saved again after the clean
				if err := indexBucket.Bucket([]byte(index.Name)).Put(ref.IndexedValue, ids.MustMarshal()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *Collection) queryGetIDs(ctx context.Context, q *Query) (*btree.BTree, error) {
	// Init the destination
	tree := btree.New(10)

	// Initialize the channel which will confirm that all queries are done
	finishedChan := make(chan *idsType, 16)
	defer close(finishedChan)

	// This count the number of running index query for this actual collection query
	nbToDo := 0

	// Goes through all index of the collection to define which index
	// will take care of the given filter
	for _, index := range c.Indexes {
		for _, filter := range q.filters {
			if index.DoesFilterApplyToIndex(filter) {
				go index.Query(ctx, filter, finishedChan)
				nbToDo++
			}
		}
	}

	if nbToDo == 0 {
		return nil, fmt.Errorf("no index found")
	}

	// Loop every response from the index query
	for {
		select {
		case tmpIDs := <-finishedChan:
			if tmpIDs != nil {
				// Add IDs into the response tree
				for _, id := range tmpIDs.IDs {
					// Try to get the id from the tree
					fromTree := tree.Get(id)
					if fromTree == nil {
						// If not in the tree add it
						id.Increment()
						tree.ReplaceOrInsert(id)
						continue
					}
					// if already increment the counter
					fromTree.(*idType).Increment()
				}
			}
			// Save the fact that one more query has been respond
			nbToDo--
			// If nomore query to wait, quit the loop
			if nbToDo <= 0 {
				return tree, nil
			}
		case <-ctx.Done():
			time.Sleep(time.Millisecond * 100)
			return nil, vars.ErrTimeOut
		}
	}
}

func (c *Collection) queryCleanAndOrder(ctx context.Context, q *Query, tree *btree.BTree) (response *ResponseQuery, _ error) {
	getRefFunc := func(id string) (refs *refs) {
		c.DB.View(func(tx *bolt.Tx) error {
			refs, _ = c.getRefs(tx, id)
			return nil
		})
		return refs
	}

	// iterate the response tree to get only IDs which has been found in every index queries
	occurrenceFunc, retTree := occurrenceTreeIterator(len(q.filters), q.internalLimit, q.order, getRefFunc)
	tree.Ascend(occurrenceFunc)

	// get the ids in the order and with the given limit
	orderFunc, ret := orderTreeIterator(q.limit)
	if q.ascendent {
		retTree.Ascend(orderFunc)
	} else {
		retTree.Descend(orderFunc)
	}

	// Build the response for the caller
	response = NewResponseQuery(len(ret.IDs))
	response.query = q
	// Get every content of the query from the database
	responsesAsBytes, err := c.get(ctx, ret.Strings()...)
	if err != nil {
		return nil, err
	}

	// Range the response values as slice of bytes
	for i := range responsesAsBytes {
		if i >= q.limit {
			break
		}

		response.List[i] = &ResponseQueryElem{
			ID:             ret.IDs[i],
			ContentAsBytes: responsesAsBytes[i],
		}
	}
	return
}
