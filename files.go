package gotinydb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

type (
	// FileMeta defines some file metadata informations
	FileMeta struct {
		ID           string
		Name         string
		Size         int64
		LastModified time.Time
		ChuckSize    int
	}

<<<<<<< HEAD
	// Reader define a simple object to read part of the file
	Reader struct {
		Meta *FileMeta

		db              *DB
		currentPosition int64
		txn             *badger.Txn
=======
	readWriter struct {
		meta            *FileMeta
		db              *DB
		currentPosition int64
		txn             *badger.Txn
		reader, writer  bool
	}

	// Reader define a simple object to read parts of the file
	Reader interface {
		io.ReadCloser
		io.Seeker
		io.ReaderAt

		// Read(p []byte) (n int, err error)
		// Seek(offset int64, whence int) (n int64, err error)
		// ReadAt(p []byte, off int64) (n int, err error)
		// Close() (err error)
		GetMeta() *FileMeta
	}

	// Writer define a simple object to write parts of the file
	Writer interface {
		Reader

		io.Writer
		io.WriterAt
>>>>>>> dev
	}
)

// PutFile let caller insert large element into the database via a reader interface
func (d *DB) PutFile(id string, name string, reader io.Reader) (n int, err error) {
	d.DeleteFile(id)

	meta := new(FileMeta)
	meta.ID = id
	meta.Name = name
	meta.Size = 0
	meta.LastModified = time.Now()
	meta.ChuckSize = fileChuckSize

	// Set the meta
	err = d.putFileMeta(id, meta)
	if err != nil {
		return
	}

	// Track the numbers of chunks
	nChunk := 1
	// Open a loop
	for true {
		// Initialize the read buffer
		buff := make([]byte, fileChuckSize)
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

		d.writeFileChunk(id, nChunk, buff)
		// ctx, cancel := context.WithCancel(context.Background())
		// defer cancel()

		// tx := transaction.NewTransaction(
		// 	ctx,
		// 	d.buildFilePrefix(id, nChunk),
		// 	buff,
		// 	false,
		// )
		// // Run the insertion
		// select {
		// case d.writeChan <- tx:
		// case <-d.ctx.Done():
		// 	return n, d.ctx.Err()
		// }

		// // And wait for the end of the insertion
		// select {
		// case err = <-tx.ResponseChan:
		// case <-tx.Ctx.Done():
		// 	err = tx.Ctx.Err()
		// }
		// if err != nil {
		// 	return
		// }

		// Increment the chunk counter
		nChunk++
	}

	meta.Size = int64(n)
	meta.LastModified = time.Now()
	err = d.putFileMeta(id, meta)
	if err != nil {
		return
	}

	err = nil
	return
}

func (d *DB) writeFileChunk(id string, chunk int, content []byte) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if fileChuckSize < len(content) {
		return fmt.Errorf("the maximum chunk size is %d bytes long but the content to write is %d bytes long", fileChuckSize, len(content))
	}

	tx := transaction.NewTransaction(
		ctx,
		d.buildFilePrefix(id, chunk),
		content,
		false,
	)
	// Run the insertion
	select {
	case d.writeChan <- tx:
	case <-d.ctx.Done():
		return d.ctx.Err()
	}

	// And wait for the end of the insertion
	select {
	case err = <-tx.ResponseChan:
	case <-tx.Ctx.Done():
		err = tx.Ctx.Err()
	}
	return
}

func (d *DB) getFileMeta(id string) (meta *FileMeta, err error) {
	meta = new(FileMeta)

	err = d.badger.View(func(txn *badger.Txn) (err error) {
		metaID := d.buildFilePrefix(id, 0)

		var item *badger.Item
		item, err = txn.Get(metaID)
		if err != nil {
			return
		}

		var valAsEncryptedBytes []byte
		valAsEncryptedBytes, err = item.ValueCopy(valAsEncryptedBytes)
		if err != nil {
			return
		}

		var valAsBytes []byte
		valAsBytes, err = cipher.Decrypt(d.PrivateKey, item.Key(), valAsEncryptedBytes)
		if err != nil {
			return
		}

		return json.Unmarshal(valAsBytes, meta)
	})
	if err != nil {
		return
	}
	return
}

<<<<<<< HEAD
	meta.Size = int64(n)
	meta.LastModified = time.Now()
	err = d.putFileMeta(id, meta)
=======
func (d *DB) putFileMeta(id string, meta *FileMeta) (err error) {
	metaID := d.buildFilePrefix(id, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var metaAsBytes []byte
	metaAsBytes, err = json.Marshal(meta)
>>>>>>> dev
	if err != nil {
		return
	}

<<<<<<< HEAD
	err = nil
=======
	tx := transaction.NewTransaction(
		ctx,
		metaID,
		metaAsBytes,
		false,
	)
	// Run the insertion
	select {
	case d.writeChan <- tx:
	case <-d.ctx.Done():
		return d.ctx.Err()
	}
	// And wait for the end of the insertion
	select {
	case err = <-tx.ResponseChan:
	case <-tx.Ctx.Done():
		err = tx.Ctx.Err()
	}
>>>>>>> dev
	return
}

func (d *DB) getFileMeta(id string) (meta *FileMeta, err error) {
	meta = new(FileMeta)

	err = d.badger.View(func(txn *badger.Txn) (err error) {
		metaID := d.buildFilePrefix(id, 0)

		var item *badger.Item
		item, err = txn.Get(metaID)
		if err != nil {
			return
		}

		var valAsEncryptedBytes []byte
		valAsEncryptedBytes, err = item.ValueCopy(valAsEncryptedBytes)
		if err != nil {
			return
		}

		var valAsBytes []byte
		valAsBytes, err = cipher.Decrypt(d.PrivateKey, item.Key(), valAsEncryptedBytes)
		if err != nil {
			return
		}

		return json.Unmarshal(valAsBytes, meta)
	})
	if err != nil {
		return
	}
	return
}

func (d *DB) putFileMeta(id string, meta *FileMeta) (err error) {
	metaID := d.buildFilePrefix(id, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var metaAsBytes []byte
	metaAsBytes, err = json.Marshal(meta)
	if err != nil {
		return
	}

	tx := transaction.NewTransaction(
		ctx,
		metaID,
		metaAsBytes,
		false,
	)
	// Run the insertion
	select {
	case d.writeChan <- tx:
	case <-d.ctx.Done():
		return d.ctx.Err()
	}
	// And wait for the end of the insertion
	select {
	case err = <-tx.ResponseChan:
	case <-tx.Ctx.Done():
		err = tx.Ctx.Err()
	}
	return
}

// ReadFile write file content into the given writer
func (d *DB) ReadFile(id string, writer io.Writer) error {
	return d.badger.View(func(txn *badger.Txn) error {
		storeID := d.buildFilePrefix(id, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchSize = 3
		opt.PrefetchValues = true

		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Seek(d.buildFilePrefix(id, 1)); it.ValidForPrefix(storeID); it.Next() {
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

<<<<<<< HEAD
// GetFileReader returns a struct to provide simple reading of big files.
func (d *DB) GetFileReader(id string) (reader *Reader, err error) {
	reader = new(Reader)

	reader.Meta, err = d.getFileMeta(id)

	reader.db = d
	reader.txn = d.badger.NewTransaction(false)

	return
=======
// GetFileReader returns a struct to provide simple reading partial of big files.
// The default position is at the begining of the file.
func (d *DB) GetFileReader(id string) (Reader, error) {
	rw, err := d.newReadWriter(id, false)
	return Reader(rw), err
}

// GetFileWriter returns a struct to provide simple partial write of big files.
// The default position is at the end of the file.
func (d *DB) GetFileWriter(id string) (Writer, error) {
	rw, err := d.newReadWriter(id, true)
	rw.currentPosition = rw.meta.Size
	return Writer(rw), err
>>>>>>> dev
}

// DeleteFile deletes every chunks of the given file ID
func (d *DB) DeleteFile(id string) (err error) {
	listOfTx := []*transaction.Transaction{}

	// Open a read transaction to get every IDs
	return d.badger.View(func(txn *badger.Txn) error {
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

<<<<<<< HEAD
// Read implements the io.Reader interface
func (r *Reader) Read(p []byte) (n int, err error) {
=======
func (d *DB) newReadWriter(id string, writer bool) (_ *readWriter, err error) {
	rw := new(readWriter)
	rw.reader = !writer
	rw.writer = writer

	rw.meta, err = d.getFileMeta(id)
	if err != nil {
		return nil, err
	}

	rw.db = d
	rw.txn = d.badger.NewTransaction(false)

	return rw, nil
}

// Read implements the io.Reader interface
func (r *readWriter) Read(p []byte) (n int, err error) {
>>>>>>> dev
	block, inside := r.getBlockAndInsidePosition(r.currentPosition)

	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 3
	opt.PrefetchValues = true

	it := r.txn.NewIterator(opt)
	defer it.Close()

	buffer := bytes.NewBuffer(nil)
	first := true

<<<<<<< HEAD
	filePrefix := r.db.buildFilePrefix(r.Meta.ID, -1)
	for it.Seek(r.db.buildFilePrefix(r.Meta.ID, block)); it.ValidForPrefix(filePrefix); it.Next() {
=======
	filePrefix := r.db.buildFilePrefix(r.meta.ID, -1)
	for it.Seek(r.db.buildFilePrefix(r.meta.ID, block)); it.ValidForPrefix(filePrefix); it.Next() {
>>>>>>> dev
		var err error
		var valAsEncryptedBytes []byte
		valAsEncryptedBytes, err = it.Item().ValueCopy(valAsEncryptedBytes)
		if err != nil {
			return 0, err
		}

		var valAsBytes []byte
		valAsBytes, err = cipher.Decrypt(r.db.PrivateKey, it.Item().Key(), valAsEncryptedBytes)
		if err != nil {
			return 0, err
		}

		var toAdd []byte
		if first {
			toAdd = valAsBytes[inside:]
		} else {
			toAdd = valAsBytes
		}
		buffer.Write(toAdd)
		if buffer.Len() >= len(p) {
			copy(p, buffer.Bytes()[:len(p)])
			r.currentPosition += int64(len(p))
			return len(p), nil
		}

		first = false
	}

	copy(p, buffer.Bytes())

	r.currentPosition = 0

	return buffer.Len(), nil
}

<<<<<<< HEAD
// Seek implements the io.Seeker interface
func (r *Reader) Seek(offset int64, whence int) (n int64, err error) {
=======
func (r *readWriter) checkReadWriteAt(off int64) error {
	if r.meta.Size <= off {
		return fmt.Errorf("the offset can not be equal or bigger than the file")
	}
	return nil
}

// ReadAt implements the io.ReaderAt interface
func (r *readWriter) ReadAt(p []byte, off int64) (n int, err error) {
	err = r.checkReadWriteAt(off)
	if err != nil {
		return 0, err
	}

	r.currentPosition = off
	return r.Read(p)
}

func (r *readWriter) getExistingBlock(blockN int) (ret []byte, err error) {
	chunkID := r.db.buildFilePrefix(r.meta.ID, blockN)
	var item *badger.Item
	item, err = r.txn.Get(chunkID)

	var valAsEncryptedBytes []byte
	valAsEncryptedBytes, err = item.ValueCopy(valAsEncryptedBytes)
	if err != nil {
		return nil, err
	}

	return cipher.Decrypt(r.db.PrivateKey, item.Key(), valAsEncryptedBytes)
}

func (r *readWriter) Write(p []byte) (n int, err error) {
	block, inside := r.getBlockAndInsidePosition(r.currentPosition)

	// chunkID := r.db.buildFilePrefix(r.meta.ID, block)
	// var item *badger.Item
	// item, err = r.txn.Get(chunkID)

	// var valAsEncryptedBytes []byte
	// valAsEncryptedBytes, err = item.ValueCopy(valAsEncryptedBytes)
	// if err != nil {
	// 	return 0, err
	// }

	var valAsBytes []byte
	valAsBytes, err = r.getExistingBlock(block)
	if err != nil {
		return 0, err
	}

	freeToWriteInThisChunk := fileChuckSize - inside
	if freeToWriteInThisChunk > len(p) {
		toWrite := valAsBytes[:inside]
		toWrite = append(toWrite, p...)
		toWrite = append(toWrite, valAsBytes[inside+len(p):]...)

		return len(p), r.db.writeFileChunk(r.meta.ID, block, toWrite)
	}

	toWriteInTheFirstChunk := valAsBytes[:inside]
	toWriteInTheFirstChunk = append(toWriteInTheFirstChunk, p[n:freeToWriteInThisChunk]...)
	err = r.db.writeFileChunk(r.meta.ID, block, toWriteInTheFirstChunk)
	if err != nil {
		return n, err
	}

	n += freeToWriteInThisChunk
	block++

	done := false

newLoop:
	newEnd := n + fileChuckSize
	if newEnd > len(p) {
		newEnd = len(p)
		done = true

	}

	nextToWrite := p[n:newEnd]
	if done {
		valAsBytes, err = r.getExistingBlock(block)
		if err != nil {
			return 0, err
		}
		nextToWrite = append(nextToWrite, valAsBytes[len(nextToWrite):]...)
	}

	err = r.db.writeFileChunk(r.meta.ID, block, nextToWrite)
	if err != nil {
		return n, err
	}

	n += fileChuckSize
	block++

	if done {
		n = len(p)
		return
	}

	goto newLoop
}

func (r *readWriter) WriteAt(p []byte, off int64) (n int, err error) {
	err = r.checkReadWriteAt(off)
	if err != nil {
		return 0, err
	}

	r.currentPosition = off
	return r.Write(p)
}

// Seek implements the io.Seeker interface
func (r *readWriter) Seek(offset int64, whence int) (n int64, err error) {
>>>>>>> dev
	switch whence {
	case io.SeekStart:
		n = offset
	case io.SeekCurrent:
		n = r.currentPosition + offset
	case io.SeekEnd:
<<<<<<< HEAD
		n = r.Meta.Size - offset
=======
		n = r.meta.Size - offset
>>>>>>> dev
	default:
		err = fmt.Errorf("whence not recognized")
	}

<<<<<<< HEAD
	if n > r.Meta.Size || n < 0 {
=======
	if n > r.meta.Size || n < 0 {
>>>>>>> dev
		err = fmt.Errorf("is out of the file")
	}

	r.currentPosition = n
	return
}

<<<<<<< HEAD
// ReadAt implements the io.ReaderAt interface
func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	if r.Meta.Size <= off {
		err = fmt.Errorf("the offset can not be equal or bigger than the file")
		return
	}

	r.currentPosition = off
	return r.Read(p)
}

// Close should be called when done with the Reader
func (r *Reader) Close() (err error) {
=======
// Close should be called when done with the Reader
func (r *readWriter) Close() (err error) {
>>>>>>> dev
	r.txn.Discard()
	return
}

<<<<<<< HEAD
func (r *Reader) getBlockAndInsidePosition(offset int64) (block, inside int) {
	return int(offset/int64(r.Meta.ChuckSize)) + 1, int(offset) % r.Meta.ChuckSize
=======
func (r *readWriter) GetMeta() *FileMeta {
	return r.meta
}

func (r *readWriter) getBlockAndInsidePosition(offset int64) (block, inside int) {
	return int(offset/int64(r.meta.ChuckSize)) + 1, int(offset) % r.meta.ChuckSize
>>>>>>> dev
}
