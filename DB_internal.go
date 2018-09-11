package gotinydb

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/dgraph-io/badger"
)

// func (d *DB) buildPath() error {
// 	return os.MkdirAll(d.options.Path+"/collections", FilePermission)
// }

func (d *DB) initBadger() error {
	if d.options.BadgerOptions == nil {
		return ErrBadBadgerConfig
	}

	opts := d.options.BadgerOptions
	opts.Dir = d.options.Path
	opts.ValueDir = d.options.Path
	db, err := badger.Open(*opts)
	if err != nil {
		return err
	}

	d.valueStore = db
	return nil
}

func (d *DB) waitForClose() {
	<-d.ctx.Done()
	d.Close()
}

func (d *DB) initWriteTransactionChan(ctx context.Context) {
	// Set a limit
	limit := d.options.PutBufferLimit
	// Build the queue with 2 times the limit to help writing on disc
	// in the same order as the operation are called
	d.writeTransactionChan = make(chan *writeTransaction, limit*2)
	// Start the infinite loop

	go d.waittingWriteLoop(ctx, limit)
}

func (d *DB) waittingWriteLoop(ctx context.Context, limit int) {
	for {
		select {
		// A request came up
		case tr := <-d.writeTransactionChan:
			// Build a new write request
			newTr := newTransaction(tr.ctx)
			// Add the first request to the waiting list
			newTr.addTransaction(tr.transactions...)

			// Build the slice of chan the writer will respond
			waittingForResponseList := []chan error{}
			// Same the first response channel
			waittingForResponseList = append(waittingForResponseList, tr.responseChan)

			// Try to empty the queue if any
		tryToGetAnOtherRequest:
			select {
			// There is an other request in the queue
			case trBis := <-d.writeTransactionChan:
				// Save the request
				newTr.addTransaction(trBis.transactions...)
				// And save the response channel
				waittingForResponseList = append(waittingForResponseList, trBis.responseChan)

				// Check if the limit is not reach
				if len(newTr.transactions) < limit {
					// If not lets try to empty the queue a bit more
					goto tryToGetAnOtherRequest
				}
				// This release continue if there is no request in the queue
			default:
			}

			// Run the write operation
			go d.writeTransactions(newTr)

			// Get the response
			err := <-newTr.responseChan
			// And spread the response to all callers in parallel
			for _, waittingForResponse := range waittingForResponseList {
				go func(waittingForResponse chan error, err error) {
					waittingForResponse <- err
				}(waittingForResponse, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (d *DB) writeTransactions(tr *writeTransaction) {
	// Build a waiting groups
	// This group is to make internal functions wait the otherone
	wgActions := new(sync.WaitGroup)
	// This group defines the waitgroup to consider that all have been done correctly.
	wgCommitted := new(sync.WaitGroup)

	// Start the new transaction
	txn := d.valueStore.NewTransaction(true)
	defer txn.Discard()

	// Used to propagate the error for one or the other function
	errChan := make(chan error, 1)
	if len(tr.transactions) == 1 {
		d.writeOneTransaction(tr.ctx, txn, errChan, wgActions, wgCommitted, tr.transactions[0])

		// Respond to the caller with the error if any
		tr.responseChan <- waitForDoneErrOrCanceled(tr.ctx, wgCommitted, errChan)
	} else {
		tr.responseChan <- d.writeMultipleTransaction(tr.ctx, txn, errChan, wgActions, wgCommitted, tr)
	}
}

func (d *DB) writeOneTransaction(ctx context.Context, txn *badger.Txn, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, wtElem *writeTransactionElement) {
	// Increment the tow waiting groups
	wgActions.Add(2)
	wgCommitted.Add(2)

	if wtElem.isInsertion {
		// Runs saving into the store
		go wtElem.collection.putIntoStore(ctx, txn, errChan, wgActions, wgCommitted, wtElem)

		// Starts the indexing process
		if !wtElem.bin {
			go wtElem.collection.putIntoIndexes(ctx, txn, errChan, wgActions, wgCommitted, wtElem)
		} else {
			go wtElem.collection.onlyCleanRefs(ctx, txn, errChan, wgActions, wgCommitted, wtElem)
		}
	} else {
		// Else is because it's a deletation
		go wtElem.collection.delFromStore(ctx, txn, errChan, wgActions, wgCommitted, wtElem)
		go wtElem.collection.onlyCleanRefs(ctx, txn, errChan, wgActions, wgCommitted, wtElem)
	}
}

func (d *DB) writeMultipleTransaction(ctx context.Context, txn *badger.Txn, errChan chan error, wgActions, wgCommitted *sync.WaitGroup, wt *writeTransaction) error {
	// Because there is only one commit for all insertion we add manually 1
	wgCommitted.Add(1)

	// Loop for every insertion
	for _, wtElem := range wt.transactions {
		// Increment the tow waiting groups
		wgActions.Add(2)

		// Build a new wait group to prevent concurant writes which make Badger panic
		var wgLoop sync.WaitGroup
		wgLoop.Add(2)

		if wtElem.isInsertion {
			// Runs saving into the store
			go wtElem.collection.putIntoStore(ctx, txn, errChan, wgActions, &wgLoop, wtElem)

			// Starts the indexing process
			if !wtElem.bin {
				go wtElem.collection.putIntoIndexes(ctx, txn, errChan, wgActions, &wgLoop, wtElem)
			} else {
				go wtElem.collection.onlyCleanRefs(ctx, txn, errChan, wgActions, &wgLoop, wtElem)
			}
		} else {
			// Else is because it's a deletation
			go wtElem.collection.delFromStore(ctx, txn, errChan, wgActions, &wgLoop, wtElem)
			go wtElem.collection.onlyCleanRefs(ctx, txn, errChan, wgActions, &wgLoop, wtElem)
		}

		// Wait for this to be saved in suspend before commit
		wgLoop.Wait()
	}

	// Tells to the rest that commit can be run now
	wgCommitted.Done()

	// Wait for error if any
	err := waitForDoneErrOrCanceled(ctx, wgCommitted, errChan)
	if err == nil {
		// Commit every thing if no error reported
		err = txn.Commit(nil)
		if err != nil {
			errChan <- err
			return err
		}
	}
	// Respond to the caller with the error if any
	return nil
}

func (d *DB) loadCollections() error {
	colsNames, getColsNamesErr := d.getCollectionsNames()
	if getColsNamesErr != nil {
		if getColsNamesErr == badger.ErrKeyNotFound {
			return nil
		}
		return getColsNamesErr
	}
	for _, colName := range colsNames {
		col, err := d.getCollection(colName)
		if err != nil {
			return err
		}

		if err := col.loadIndex(); err != nil {
			return err
		}

		d.collections = append(d.collections, col)
	}

	return nil
}

func (d *DB) getCollection(colName string) (*Collection, error) {
	c := new(Collection)
	c.store = d.valueStore
	c.name = colName
	c.writeTransactionChan = d.writeTransactionChan

	c.options = d.options

	if !d.isColExists(colName) {
		c.prefix = d.getNextColPrefix()
	}

	c.name = colName
	c.ctx = d.ctx
	// Try to load the collection information
	if err := c.loadInfos(); err != nil {
		// If not exists try to build it
		if err == badger.ErrKeyNotFound {
			err = c.init(colName)
			// Error after at build
			if err != nil {
				return nil, err
			}
			// No error return the new Collection pointer
			return c, nil
		}
		// Other error than not found
		return nil, err
	}

	// The collection is loaded and database is ready
	return c, nil
}

func (d *DB) getCollectionsNames() ([]string, error) {
	var ret []string
	err := d.valueStore.View(func(txn *badger.Txn) error {
		colsAsItem, err := txn.Get(d.buildIDWithCollectionsInfoPrefix([]byte(_IDCollectionsInfoCollectionsNames)))
		if err != nil {
			return err
		}

		var colsAsBytes []byte
		colsAsBytes, err = colsAsItem.Value()
		if err != nil {
			return err
		}
		return json.Unmarshal(colsAsBytes, &ret)
	})
	return ret, err
}

// func (d *DB) getCollectionsIDs() ([]string, error) {
// 	ret := []string{}

// 	d.valueStore.View(func(txn *badger.Txn) error {
// 		opt := badger.DefaultIteratorOptions
// 		opt.PrefetchValues = false
// 		it := txn.NewIterator(opt)
// 		defer it.Close()
// 		colPrefix := d.buildIDWithCollectionsInfoPrefix(nil)
// 		for it.Seek(colPrefix); it.ValidForPrefix(colPrefix); it.Next() {
// 			ret = append(ret, string(it.Item().Key()))
// 		}
// 		return nil
// 	})
// 	// files, err := ioutil.ReadDir(d.options.Path + "/collections")
// 	// if err != nil {
// 	// 	return nil, err
// 	// }
// 	//
// 	// for _, f := range files {
// 	// 	ret = append(ret, f.Name())
// 	// }

// 	return ret, nil
// }

func (d *DB) buildIDWithCollectionsInfoPrefix(id []byte) []byte {
	ret := []byte{prefixCollectionsInfo}
	return append(ret, id...)
}

func (d *DB) isColExists(colName string) bool {
	ret := false
	d.valueStore.View(func(txn *badger.Txn) error {
		item, err := txn.Get(d.buildIDWithCollectionsInfoPrefix([]byte(_IDCollectionsInfoCollectionsNames)))
		if err != nil {
			return err
		}

		var asBytes []byte
		asBytes, err = item.Value()
		if err != nil {
			return err
		}

		var names []string
		err = json.Unmarshal(asBytes, &names)
		if err != nil {
			return err
		}

		for _, name := range names {
			if name == colName {
				ret = true
				break
			}
		}

		return nil
	})

	return ret
}

func (d *DB) getNextColPrefix() byte {
	ret := byte(0)
	d.valueStore.View(func(txn *badger.Txn) error {
		item, err := txn.Get(d.buildIDWithCollectionsInfoPrefix([]byte(_IDCollectionsInfoCollectionsNames)))
		if err != nil {
			return err
		}

		var asBytes []byte
		asBytes, err = item.Value()
		if err != nil {
			return err
		}

		var names []string
		err = json.Unmarshal(asBytes, &names)
		if err != nil {
			return err
		}

		count := 0
		for range names {
			count++
		}

		ret = byte(count)

		return nil
	})

	return ret
}
