package simple

import (
	"context"
	"io"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/debug/simple/transaction"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

// PutFile let caller insert large element into the database via a reader interface
func (d *DB) PutFile(id string, reader io.Reader) (n int, err error) {
	d.DeleteFile(id)

	// Track the numbers of chunks
	nChunk := 0
	// Open a loop
	for true {
		// Initialize the read buffer
		buff := make([]byte, FileChuckSize)
		var nWritten int
		nWritten, err = reader.Read(buff)
		// The read is done and it returns
		if nWritten == 0 || err == io.EOF && nWritten == 0 {
			break
		}
		// Return error if any
		if err != nil && err != io.EOF {
			return
		}

		// Clean the buffer
		buff = buff[:nWritten]

		n = n + nWritten

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tx := transaction.NewTransaction(
			ctx,
			d.buildFilePrefix(id, nChunk),
			buff,
			false,
		)
		// Run the insertion
		d.writeChan <- tx
		// And wait for the end of the insertion
		select {
		case err = <-tx.ResponseChan:
		case <-tx.Ctx.Done():
			err = tx.Ctx.Err()
		}
		if err != nil {
			return
		}

		// Increment the chunk counter
		nChunk++
	}

	err = nil
	return
}

// ReadFile write file content into the given writer
func (d *DB) ReadFile(id string, writer io.Writer) error {
	return d.Badger.View(func(txn *badger.Txn) error {
		storeID := d.buildFilePrefix(id, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchSize = 3
		opt.PrefetchValues = true

		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			var err error
			var valAsEncryptedBytes []byte
			valAsEncryptedBytes, err = it.Item().ValueCopy(valAsEncryptedBytes)
			if err != nil {
				return err
			}

			var valAsBytes []byte
			valAsBytes, err = cipher.Decrypt(d.PrivateKey, it.Item().Key(), valAsEncryptedBytes)
			if err != nil {
				return err
			}

			_, err = writer.Write(valAsBytes)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// DeleteFile deletes every chunks of the given file ID
func (d *DB) DeleteFile(id string) (err error) {
	listOfTx := []*transaction.Transaction{}

	// Open a read transaction to get every IDs
	return d.Badger.View(func(txn *badger.Txn) error {
		// Build the file prefix
		storeID := d.buildFilePrefix(id, -1)

		// Defines the iterator options to get only IDs
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		// Initialize the iterator
		it := txn.NewIterator(opt)
		defer it.Close()

		// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Go the the first file chunk
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			// Copy the store key
			var key []byte
			key = it.Item().KeyCopy(key)
			// And add it to the list of store IDs to delete
			tx := transaction.NewTransaction(ctx, key, nil, true)
			listOfTx = append(listOfTx, tx)
			d.writeChan <- tx
		}

		for _, tx := range listOfTx {
			select {
			case err = <-tx.ResponseChan:
			case <-tx.Ctx.Done():
				err = tx.Ctx.Err()
			}
			if err != nil {
				return err
			}
		}

		// Close the view transaction
		return nil
	})

	// // Start the write operation and returns the error if any
	// return d.badgerDB.Update(func(txn *badger.Txn) error {
	// 	// Loop for every IDs to remove and remove it
	// 	for _, id := range idsToDelete {
	// 		err := txn.Delete(id)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// })
}

func (d *DB) buildFilePrefix(id string, chunkN int) []byte {
	// Derive the ID to make sure no file ID overlap the other.
	// Because the files are chunked it needs to have a stable prefix for reading
	// and deletation.
	derivedID := blake2b.Sum256([]byte(id))

	// Build the prefix
	prefixWithID := append([]byte{prefixFiles}, derivedID[:]...)

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
