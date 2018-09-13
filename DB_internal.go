package gotinydb

import (
	"context"
	"encoding/json"

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

// func (d *DB) waitForClose() {
// 	<-d.ctx.Done()
// 	d.Close()
// }

func (d *DB) initWriteTransactionChan(ctx context.Context) {
	// Set a limit
	limit := d.options.PutBufferLimit
	// Build the queue with 2 times the limit to help writing on disc
	// in the same order as the operation are called
	d.writeTransactionChan = make(chan *writeTransaction, limit*2)
	// Start the infinite loop

	go d.waittingWriteLoop(ctx, limit)
}

func (d *DB) initCollection(name string) (*Collection, error) {
	c := new(Collection)
	c.name = name

	// Set the prefix
	c.prefix = d.freePrefix[0]

	// Remove the prefix from the list of free prefixes
	d.freePrefix = append(d.freePrefix[:0], d.freePrefix[1:]...)

	// Set the different values af the collection
	c.store = d.valueStore
	c.writeTransactionChan = d.writeTransactionChan
	c.ctx = d.ctx
	c.options = d.options

	d.collections = append(d.collections, c)

	c.saveCollections = d.saveCollections

	return c, nil
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
			err := d.writeTransactions(newTr)

			// // Get the response
			// err := <-newTr.responseChan
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

func (d *DB) writeTransactions(tr *writeTransaction) error {
	// Start the new transaction
	txn := d.valueStore.NewTransaction(true)
	defer txn.Discard()

	var err error

	if len(tr.transactions) == 1 {
		// Respond to the caller with the error if any
		// return d.writeOneTransaction(tr.ctx, txn, tr.transactions[0])
		err := d.writeOneTransaction(tr.ctx, txn, tr.transactions[0])
		// tr.responseChan <- err
		if err != nil {
			return err
		}

		goto commit
	}

	err = d.writeMultipleTransaction(tr.ctx, txn, tr)
	// tr.responseChan <- err
	if err != nil {
		return err
	}

commit:
	return txn.Commit(nil)
}

func (d *DB) writeOneTransaction(ctx context.Context, txn *badger.Txn, wtElem *writeTransactionElement) error {
	if wtElem.isInsertion {
		// Runs saving into the store
		err := wtElem.collection.insertOrDeleteStore(ctx, txn, true, wtElem)
		if err != nil {
			return err
		}

		// Starts the indexing process
		if !wtElem.bin {
			return wtElem.collection.putIntoIndexes(ctx, txn, wtElem)
			// err = wtElem.collection.putIntoIndexes(ctx, txn, wtElem)
			// if err != nil {
			// 	return err
			// }
		}

		// } else {
		return wtElem.collection.cleanRefs(ctx, txn, wtElem.id)
		// err = wtElem.collection.cleanRefs(ctx, txn, wtElem.id)
		// if err != nil {
		// 	return err
		// }
	}
	// Else is because it's a deletation
	err := wtElem.collection.insertOrDeleteStore(ctx, txn, false, wtElem)
	if err != nil {
		return err
	}
	return wtElem.collection.cleanRefs(ctx, txn, wtElem.id)
	// err = wtElem.collection.cleanRefs(ctx, txn, wtElem.id)
	// if err != nil {
	// 	return err
	// }
}

func (d *DB) writeMultipleTransaction(ctx context.Context, txn *badger.Txn, wt *writeTransaction) error {
	// Loop for every insertion
	for _, wtElem := range wt.transactions {
		err := d.writeOneTransaction(ctx, txn, wtElem)
		if err != nil {
			return err
		}
		// if wtElem.isInsertion {
		// 	// Runs saving into the store
		// 	err := wtElem.collection.putIntoStore(ctx, txn, wtElem)
		// 	if err != nil {
		// 		return err
		// 	}

		// 	// Starts the indexing process
		// 	if !wtElem.bin {
		// 		err = wtElem.collection.putIntoIndexes(ctx, txn, wtElem)
		// 		if err != nil {
		// 			return err
		// 		}
		// 	} else {
		// 		err = wtElem.collection.onlyCleanRefs(ctx, txn, wtElem)
		// 		if err != nil {
		// 			return err
		// 		}
		// 	}
		// } else {
		// 	// Else is because it's a deletation
		// 	err := wtElem.collection.delFromStore(ctx, txn, wtElem)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	err = wtElem.collection.onlyCleanRefs(ctx, txn, wtElem)
		// 	if err != nil {
		// 		return err
		// 	}
		// }
	}
	return nil
}

func (d *DB) loadCollections() error {
	return d.valueStore.View(func(txn *badger.Txn) error {
		// Get the config
		item, err := txn.Get(configID)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return d.initDB()
			} else {
				return err
			}
			// return err
		}
		var configAsBytes []byte
		configAsBytes, err = item.Value()
		if err != nil {
			return err
		}

		// Convert the saved JSON config to object
		savedDB := new(dbExport)
		err = json.Unmarshal(configAsBytes, savedDB)
		if err != nil {
			return err
		}

		// Save the free prefixes
		d.freePrefix = savedDB.FreePrefix

		// Fill up collections
		for _, savedCol := range savedDB.Collections {
			newCol := new(Collection)
			newCol.name = savedCol.Name
			newCol.prefix = savedCol.Prefix

			newCol.indexes = savedCol.Indexes
			for _, i := range newCol.indexes {
				i.options = d.options
				i.getTx = d.valueStore.NewTransaction
				i.getIDBuilder = func(id []byte) []byte {
					return newCol.buildIDWhitPrefixIndex([]byte(i.Name), id)
				}
			}

			newCol.store = d.valueStore
			newCol.writeTransactionChan = d.writeTransactionChan
			newCol.ctx = d.ctx
			newCol.options = d.options

			d.collections = append(d.collections, newCol)
		}

		return nil
	})
	// colsNames, getColsNamesErr := d.getCollectionsNames()
	// if getColsNamesErr != nil {
	// 	if getColsNamesErr == badger.ErrKeyNotFound {
	// 		return nil
	// 	}
	// 	return getColsNamesErr
	// }
	// for _, colName := range colsNames {
	// 	col, err := d.getCollection(colName)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	if err := col.loadIndex(); err != nil {
	// 		return err
	// 	}

	// 	d.collections = append(d.collections, col)
	// }
}

func (d *DB) saveCollections() error {
	return d.valueStore.Update(func(txn *badger.Txn) error {
		dbToSave := new(dbExport)
		// Save the free prefixes
		dbToSave.FreePrefix = d.freePrefix

		// Save collections
		for _, col := range d.collections {
			colToSave := new(collectionExport)
			colToSave.Name = col.name
			colToSave.Prefix = col.prefix
			colToSave.Indexes = col.indexes

			dbToSave.Collections = append(dbToSave.Collections, colToSave)
		}

		// Convert to JSON
		dbToSaveAsBytes, err := json.Marshal(dbToSave)
		if err != nil {
			return err
		}

		e := &badger.Entry{
			Key:   configID,
			Value: dbToSaveAsBytes,
		}

		copyOfKey := make([]byte, len(e.Key))
		copy(copyOfKey, e.Key)
		copyOfValue := make([]byte, len(e.Value))
		copy(copyOfValue, e.Value)
		e.Key = copyOfKey
		e.Value = copyOfValue

		return txn.SetEntry(e)
		// return txn.Set(configID, dbToSaveAsBytes)
	})
}

func (d *DB) initDB() error {
	d.freePrefix = make([]byte, 255)
	for i := 1; i < 256; i++ {
		d.freePrefix[i-1] = byte(i)
	}

	return nil
}

// func (d *DB) getCollection(colName string) (*Collection, error) {
// 	c := new(Collection)
// 	c.store = d.valueStore
// 	c.name = colName
// 	c.writeTransactionChan = d.writeTransactionChan

// 	c.options = d.options

// 	if !d.isColExists(colName) {
// 		c.prefix = d.getNextColPrefix()
// 	}

// 	c.name = colName
// 	c.ctx = d.ctx
// 	// Try to load the collection information
// 	if err := c.loadInfos(); err != nil {
// 		// If not exists try to build it
// 		if err == badger.ErrKeyNotFound {
// 			err = c.init(colName)
// 			// Error after at build
// 			if err != nil {
// 				return nil, err
// 			}
// 			// No error return the new Collection pointer
// 			return c, nil
// 		}
// 		// Other error than not found
// 		return nil, err
// 	}

// 	// The collection is loaded and database is ready
// 	return c, nil
// }

// func (d *DB) getCollectionsNames() ([]string, error) {
// 	var ret []string
// 	err := d.valueStore.View(func(txn *badger.Txn) error {
// 		colsAsItem, err := txn.Get(d.buildIDWithCollectionsInfoPrefix([]byte(_IDCollectionsInfoCollectionsNames)))
// 		if err != nil {
// 			return err
// 		}

// 		var colsAsBytes []byte
// 		colsAsBytes, err = colsAsItem.Value()
// 		if err != nil {
// 			return err
// 		}
// 		return json.Unmarshal(colsAsBytes, &ret)
// 	})
// 	return ret, err
// }

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

// func (d *DB) buildIDWithCollectionsInfoPrefix(id []byte) []byte {
// 	ret := []byte{prefixCollectionsInfo}
// 	return append(ret, id...)
// }

// func (d *DB) isColExists(colName string) bool {
// 	ret := false
// 	d.valueStore.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get(d.buildIDWithCollectionsInfoPrefix([]byte(_IDCollectionsInfoCollectionsNames)))
// 		if err != nil {
// 			return err
// 		}

// 		var asBytes []byte
// 		asBytes, err = item.Value()
// 		if err != nil {
// 			return err
// 		}

// 		var names []string
// 		err = json.Unmarshal(asBytes, &names)
// 		if err != nil {
// 			return err
// 		}

// 		for _, name := range names {
// 			if name == colName {
// 				ret = true
// 				break
// 			}
// 		}

// 		return nil
// 	})

// 	return ret
// }

// func (d *DB) getNextColPrefix() byte {
// 	ret := byte(0)
// 	d.valueStore.View(func(txn *badger.Txn) error {
// 		item, err := txn.Get(d.buildIDWithCollectionsInfoPrefix([]byte(_IDCollectionsInfoCollectionsNames)))
// 		if err != nil {
// 			return err
// 		}

// 		var asBytes []byte
// 		asBytes, err = item.Value()
// 		if err != nil {
// 			return err
// 		}

// 		var names []string
// 		err = json.Unmarshal(asBytes, &names)
// 		if err != nil {
// 			return err
// 		}

// 		count := 0
// 		for range names {
// 			count++
// 		}

// 		ret = byte(count)

// 		return nil
// 	})

// 	return ret
// }
