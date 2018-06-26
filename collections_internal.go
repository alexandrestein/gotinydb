package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/boltdb/bolt"
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
		if err := confBucket.Put([]byte("name"), []byte(name)); err != nil {
			return err
		}
		if err := confBucket.Put([]byte("id"), []byte(c.ID)); err != nil {
			return err
		}
		return nil
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

// func (c *Collection) initTransactionTickets(ctx context.Context) {
// 	c.startTransactionTicket = make(chan bool, 0)
// 	c.endTransactionTicket = make(chan bool, c.nbTransactionLimit)

// 	go func() {
// 		for {
// 			if c.nbTransaction < c.nbTransactionLimit {
// 				select {
// 				case c.startTransactionTicket <- true:
// 					// Unlock the caller of Collection.startTransaction
// 					c.nbTransaction++
// 				case <-c.endTransactionTicket:
// 					// In case a transaction is done
// 					c.nbTransaction--
// 				case <-ctx.Done():
// 					return
// 				}
// 			} else {
// 				select {
// 				case <-c.endTransactionTicket:
// 					c.nbTransaction--
// 				case <-ctx.Done():
// 					return
// 				}
// 			}
// 		}
// 	}()
// }

func (c *Collection) putTransaction(tr *writeTransaction) {
	storeDoneChannel := make(chan bool, 0)
	indexDoneChannel := make(chan bool, 0)
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
	return c.DB.Update(func(tx *bolt.Tx) error {
		err := c.cleanRefs(ctx, tx, writeTransaction.id)
		if err != nil {
			return err
		}

		refsBucket := tx.Bucket([]byte("refs"))
		refsAsBytes := refsBucket.Get(vars.BuildBytesID(writeTransaction.id))
		refs := NewRefs()
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
				ids, parseIDsErr := NewIDs(ctx, 0, nil, idsAsBytes)
				if parseIDsErr != nil {
					errChan <- parseIDsErr
					return parseIDsErr
				}

				id := NewID(ctx, writeTransaction.id)
				ids.AddID(id)
				idsAsBytes = ids.MustMarshal()

				if err := indexBucket.Put(indexedValue, idsAsBytes); err != nil {
					errChan <- err
					return err
				}

				refs.SetIndexedValue(index.Name, index.selectorHash, indexedValue)
			}
		}

		putErr := refsBucket.Put(refs.IDasBytes(), refs.AsBytes())
		if putErr != nil {
			errChan <- err
			return err
		}

		// 	close(indexErrChan)

		// 	nbTry := 0
		// waitForEnd:
		// 	select {
		// 	case err, ok := <-storeErrChan:
		// 		if !ok {
		// 			return nil
		// 		}
		// 		if err != nil {
		// 			return fmt.Errorf("issue on the store: %s", err.Error())
		// 		}
		// 		return nil
		// 	case <-ctx.Done():
		// 		if writeTransaction.done {
		// 			if nbTry < 5 {
		// 				goto waitForEnd
		// 			}
		// 			nbTry++
		// 		}
		// 		return ctx.Err()
		// 	}
		return c.endOfIndexUpdate(ctx, errChan, doneChan, writeTransaction)
	})
}

func (c *Collection) onlyCleanRefs(ctx context.Context, errChan chan error, doneChan chan bool, writeTransaction *writeTransaction) error {
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
		fmt.Println("error from outsid of the index")
		return fmt.Errorf("error from outsid of the index")
	case <-ctx.Done():
		fmt.Println("bizare index", ctx.Err())
		return ctx.Err()
	}
}

func (c *Collection) cleanRefs(ctx context.Context, tx *bolt.Tx, idAsString string) error {
	indexBucket := tx.Bucket([]byte("indexes"))
	refsBucket := tx.Bucket([]byte("refs"))

	// Get the references of the given ID
	refsAsBytes := refsBucket.Get(vars.BuildBytesID(idAsString))
	refs := NewRefs()
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
				ids, newIDErr := NewIDs(ctx, 0, nil, indexBucket.Bucket([]byte(index.Name)).Get(ref.IndexedValue))
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
