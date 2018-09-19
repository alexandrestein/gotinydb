package gotinydb

import (
	"context"
	"encoding/json"

	"github.com/minio/highwayhash"

	"github.com/dgraph-io/badger"
)

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

	d.badgerDB = db
	return nil
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

func (d *DB) initCollection(name string) (*Collection, error) {
	c := new(Collection)
	c.name = name

	// Set the prefix
	c.prefix = d.freePrefix[0]

	// Remove the prefix from the list of free prefixes
	d.freePrefix = append(d.freePrefix[:0], d.freePrefix[1:]...)

	// Set the different values af the collection
	c.store = d.badgerDB
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
				// This continue if there is no more request in the queue
			default:
			}

			// Run the write operation
			err := d.writeTransactions(newTr)

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
	txn := d.badgerDB.NewTransaction(true)
	defer txn.Discard()

	var err error

	if len(tr.transactions) == 1 {
		// Respond to the caller with the error if any
		err := d.writeOneTransaction(tr.ctx, txn, tr.transactions[0])
		if err != nil {
			return err
		}

		goto commit
	}

	err = d.writeMultipleTransaction(tr.ctx, txn, tr)
	if err != nil {
		return err
	}

commit:
	return txn.Commit(nil)
}

func (d *DB) writeOneTransaction(ctx context.Context, txn *badger.Txn, wtElem *writeTransactionElement) error {
	if wtElem.isFile {
		return d.insertOrDeleteFileChunks(ctx, txn, wtElem)
	} else if wtElem.isInsertion {
		// Runs saving into the store
		err := wtElem.collection.insertOrDeleteStore(ctx, txn, true, wtElem)
		if err != nil {
			return err
		}

		// Starts the indexing process
		if !wtElem.bin {
			if len(wtElem.collection.indexes) > 0 {
				return wtElem.collection.putIntoIndexes(ctx, txn, wtElem)
			}
		}
		return wtElem.collection.cleanRefs(ctx, txn, wtElem.id)
	}
	// Else is because it's a deletation
	err := wtElem.collection.insertOrDeleteStore(ctx, txn, false, wtElem)
	if err != nil {
		return err
	}
	return wtElem.collection.cleanRefs(ctx, txn, wtElem.id)
}

func (d *DB) writeMultipleTransaction(ctx context.Context, txn *badger.Txn, wt *writeTransaction) error {
	// Loop for every insertion
	for _, wtElem := range wt.transactions {
		err := d.writeOneTransaction(ctx, txn, wtElem)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) loadCollections() error {
	return d.badgerDB.View(func(txn *badger.Txn) error {
		// Get the config
		item, err := txn.Get(configID)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return d.initDB()
			}
			return err
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
			newCol.store = d.badgerDB
			newCol.writeTransactionChan = d.writeTransactionChan
			newCol.ctx = d.ctx
			newCol.options = d.options
			for _, tmpIndex := range savedCol.Indexes {
				i := new(indexType)

				i.Name = tmpIndex.Name
				i.Selector = tmpIndex.Selector
				i.Type = tmpIndex.Type

				i.options = d.options
				i.getTx = d.badgerDB.NewTransaction
				i.getIDBuilder = func(id []byte) []byte {
					return newCol.buildIDWhitPrefixIndex([]byte(i.Name), id)
				}

				newCol.indexes = append(newCol.indexes, i)
			}

			d.collections = append(d.collections, newCol)
		}

		return nil
	})
}

func (d *DB) saveCollections() error {
	return d.badgerDB.Update(func(txn *badger.Txn) error {
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

		return txn.SetEntry(e)
	})
}

func (d *DB) initDB() error {
	d.freePrefix = make([]byte, 255)
	for i := 1; i < 256; i++ {
		d.freePrefix[i-1] = byte(i)
	}

	return nil
}

func (d *DB) buildFilePrefix(id string, chunkN int) []byte {
	// Derive the ID to make sure no file ID overlap the other.
	// Because the files are chunked it needs to have a stable prefix for reading
	// and deletation.
	derivedID := highwayhash.Sum([]byte(id), make([]byte, 32))

	// Build the prefix
	prefixWithID := append([]byte{prefixFile}, derivedID[:]...)

	// Initialize the chunk part of the ID
	chunkPart := []byte{}

	// If less than zero it for deletation and only the prefix is returned
	if chunkN < 0 {
		return prefixWithID
	}

	// If it's the first chunk
	if chunkN == 0 {
		chunkPart = append(chunkPart, 0)
	} else {
		// Lockup the numbers of full bytes for the chunk ID
		nbFull := chunkN / 256
		restFull := chunkN % 256

		for index := 0; index < nbFull; index++ {
			chunkPart = append(chunkPart, 255)
		}
		chunkPart = append(chunkPart, uint8(restFull))
	}

	// Return the ID for the given file and ID
	return append(prefixWithID, chunkPart...)
}

func (d *DB) insertOrDeleteFileChunks(ctx context.Context, txn *badger.Txn, wtElem *writeTransactionElement) error {
	if wtElem.isInsertion {
		storeID := d.buildFilePrefix(wtElem.id, wtElem.chunkN)
		return txn.Set(storeID, wtElem.contentAsBytes)
	}
	return nil
}
