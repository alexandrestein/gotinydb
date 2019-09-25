/*
Package gotinydb implements a simple but useful embedded database.

It supports document insertion and retrieving of golang pointers via the JSON package.
Those documents can be indexed with Bleve.

File management is also supported and the all database is encrypted.

It relais on Bleve and Badger to do the job.
*/
package gotinydb

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/alexandrestein/gotinydb/blevestore"
	"github.com/alexandrestein/gotinydb/cipher"
	"github.com/alexandrestein/gotinydb/transaction"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"golang.org/x/crypto/blake2b"

	_ "github.com/blevesearch/bleve/analysis/analyzer/keyword"
	_ "github.com/blevesearch/bleve/analysis/analyzer/simple"
	_ "github.com/blevesearch/bleve/analysis/analyzer/standard"
)

type (
	// DB is the base struct of the package.
	// It provides the collection and manage all writes to the database.
	DB struct {
		ctx    context.Context
		cancel context.CancelFunc

		lock *sync.RWMutex

		// Only used to save database settup
		configKey [32]byte
		// PrivateKey is public for marshaling reason and should never by used or changes.
		// This is the primary key used to derive every records.
		privateKey [32]byte

		path   string
		badger *badger.DB
		// Collection is public for marshaling reason and should never be used.
		// It contains the collections pointers used to manage the documents.
		collections []*Collection

		// FileStore provides all accessibility to the file storage facilities
		fileStore *FileStore

		writeChan chan *transaction.Transaction
	}

	dbExport struct {
		Collections []*Collection
		PrivateKey  [32]byte
	}

	dbElement struct {
		name string
		// Prefix defines the all prefix to the values
		prefix []byte
	}
)

// Name simply returns the name of the element
func (dr *dbElement) Name() string {
	return dr.name
}

func init() {
	// This should prevent indexing the not indexed values
	mapping.StoreDynamic = false
	mapping.DocValuesDynamic = false
}

// Open initialize a new database or open an existing one.
// The path defines the place the data will be saved and the configuration key
// permit to decrypt existing configuration and to encrypt new one.
func Open(path string, configKey [32]byte) (db *DB, err error) {
	return open(path, configKey, nil)
}

// OpenReadOnly open the given database in readonly mode
func OpenReadOnly(path string, configKey [32]byte) (db *DB, err error) {
	option := badger.DefaultOptions(path)
	option.ReadOnly = true

	return open(path, configKey, &option)
}

func open(path string, configKey [32]byte, badgerOptions *badger.Options) (db *DB, err error) {
	db = new(DB)
	db.path = path
	db.configKey = configKey

	db.lock = new(sync.RWMutex)

	db.ctx, db.cancel = context.WithCancel(context.Background())

	db.fileStore = &FileStore{db}

	if badgerOptions == nil {
		tmpOption := badger.DefaultOptions(path)

		tmpOption = tmpOption.WithMaxTableSize(int64(FileChuckSize) / 5)     // 1MB
		tmpOption = tmpOption.WithValueLogFileSize(int64(FileChuckSize) * 4) // 20MB
		tmpOption = tmpOption.WithNumCompactors(runtime.NumCPU())
		tmpOption = tmpOption.WithTruncate(true)
		// Keep as much version as possible
		tmpOption = tmpOption.WithNumVersionsToKeep(math.MaxInt32)
		tmpOption = tmpOption.WithLogger(new(fakeLogger))

		badgerOptions = &tmpOption
	}

	db.writeChan = make(chan *transaction.Transaction, 1000)

	db.badger, err = badger.Open(*badgerOptions)
	if err != nil {
		return nil, err
	}
	db.startBackgroundLoops()

	err = db.loadConfig()
	if err != nil {
		return nil, err
	} else {
		err = db.loadCollections()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (d *DB) startBackgroundLoops() {
	go d.goRoutineLoopForWrites()
	go d.goRoutineLoopForGC()
	go d.goWatchForTTLToClean()
}

// GetCollections returns a slice of the collections name
func (d *DB) GetCollections() []string {
	d.lock.RLock()
	defer d.lock.RUnlock()

	ret := make([]string, len(d.collections))
	for i, col := range d.collections {
		ret[i] = col.GetName()
	}

	return ret
}

// GetFileStore returns a slice of the collections name
func (d *DB) GetFileStore() *FileStore {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return d.fileStore
}

// Use build a new collection or open an existing one.
func (d *DB) Use(colName string) (col *Collection, err error) {
	tmpHash := blake2b.Sum256([]byte(colName))
	prefix := append([]byte{prefixCollections}, tmpHash[:2]...)
	for _, savedCol := range d.collections {
		if savedCol.name == colName {
			if savedCol.db == nil {
				savedCol.db = d
			}
			col = savedCol
		} else if reflect.DeepEqual(savedCol.prefix, prefix) {
			return nil, ErrHashCollision
		}
	}

	if col != nil {
		return col, nil
	}

	col = newCollection(colName)
	col.prefix = prefix
	col.db = d

	d.collections = append(d.collections, col)

	err = d.saveConfig()
	if err != nil {
		return nil, err
	}

	return
}

// UpdateKey updates the database master key
func (d *DB) UpdateKey(newKey [32]byte) (err error) {
	d.configKey = newKey

	return d.saveConfig()
}

// Close closes the database and all subcomposants. It returns the error if any
func (d *DB) Close() (err error) {
	d.cancel()

	// In case of any error
	defer func() {
		if err != nil {
			d.badger.Close()
		}
	}()

	for _, col := range d.collections {
		for _, i := range col.BleveIndexes {
			err = i.close()
			if err != nil {
				return err
			}
		}
	}

	return d.badger.Close()
}

// Backup perform a full backup of the database.
// It fills up the io.Writer with all data indexes and configurations.
func (d *DB) BackupClearSince(w io.Writer) (lastTimeStamp uint64, _ error) {
	stream := d.badger.NewStream()
	stream.KeyToList = func(key []byte, itr *badger.Iterator) (*pb.KVList, error) {
		list := &pb.KVList{}
		for ; itr.Valid(); itr.Next() {
			item := itr.Item()
			if !bytes.Equal(item.Key(), key) {
				return list, nil
			}

			id := item.KeyCopy(nil)

			var valCopy []byte
			if !item.IsDeletedOrExpired() {
				if len(id) == 1 && id[0] == prefixConfig {
					conf, err := d.getConfigValue()
					if err != nil {
						return nil, err
					}
					fmt.Println("conf", conf.Collections)

					continue
				}

				// No need to copy value, if item is deleted or expired.
				encryptedValCopy, err := item.ValueCopy(nil)
				if err != nil {
					return nil, err
				}

				valCopy, err = d.decryptData(id, encryptedValCopy)
				if err != nil {
					return nil, err
				}
			}

			kv := &pb.KV{
				Key:       id,
				Value:     valCopy,
				UserMeta:  []byte{item.UserMeta()},
				Version:   item.Version(),
				ExpiresAt: item.ExpiresAt(),
			}
			list.Kv = append(list.Kv, kv)

			if item.IsDeletedOrExpired() {

				return list, nil
			}
		}
		return list, nil
	}

	var maxVersion uint64
	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.Kv {
			if maxVersion < kv.Version {
				maxVersion = kv.Version
			}
		}
		return writeTo(list, w)
	}

	if err := stream.Orchestrate(context.Background()); err != nil {
		return 0, err
	}
	return maxVersion, nil
}

func writeTo(list *pb.KVList, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint64(list.Size())); err != nil {
		return err
	}
	buf, err := list.Marshal()
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (d *DB) loadClear(r io.Reader) error {
	br := bufio.NewReaderSize(r, 16<<10)
	unmarshalBuf := make([]byte, 1<<10)

	ldr := d.badger.NewKVLoader(1000)
	for {
		var sz uint64
		err := binary.Read(br, binary.LittleEndian, &sz)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if cap(unmarshalBuf) < int(sz) {
			unmarshalBuf = make([]byte, sz)
		}

		if _, err = io.ReadFull(br, unmarshalBuf[:sz]); err != nil {
			return err
		}

		list := &pb.KVList{}
		if err := list.Unmarshal(unmarshalBuf[:sz]); err != nil {
			return err
		}

		for _, kv := range list.Kv {
			clearValue := make([]byte, len(kv.Value))
			copy(clearValue, kv.Value)

			kv.Value = cipher.Encrypt(d.privateKey, kv.GetKey(), clearValue)

			if err := ldr.Set(kv); err != nil {
				return err
			}
		}
	}

	if err := ldr.Finish(); err != nil {
		return err
	}
	return nil
}

// GarbageCollection provides access to the garbage collection for the underneath database storeage (Badger).
//
// RunValueLogGC triggers a value log garbage collection.
//
// It picks value log files to perform GC based on statistics that are collected duing compactions. If no such statistics are available, then log files are picked in random order. The process stops as soon as the first log file is encountered which does not result in garbage collection.
// When a log file is picked, it is first sampled. If the sample shows that we can discard at least discardRatio space of that file, it would be rewritten.
// If a call to RunValueLogGC results in no rewrites, then an ErrNoRewrite is thrown indicating that the call resulted in no file rewrites.
// We recommend setting discardRatio to 0.5, thus indicating that a file be rewritten if half the space can be discarded. This results in a lifetime value log write amplification of 2 (1 from original write + 0.5 rewrite + 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer space reclaims, while setting it to a lower value would result in more space reclaims at the cost of increased activity on the LSM tree. discardRatio must be in the range (0.0, 1.0), both endpoints excluded, otherwise an ErrInvalidRequest is returned.
// Only one GC is allowed at a time. If another value log GC is running, or DB has been closed, this would return an ErrRejected.
// Note: Every time GC is run, it would produce a spike of activity on the LSM tree.
func (d *DB) GarbageCollection(discardRatio float64) error {
	if discardRatio <= 0 || discardRatio >= 1 {
		discardRatio = 0.5
	}

	err := d.badger.RunValueLogGC(discardRatio)
	if err != nil {
		return err
	}

	return d.badger.Sync()
}

// Load recover an existing database from a backup generated with *DB.Backup
func (d *DB) LoadClear(r io.Reader) error {
	err := d.badger.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte{prefixConfig})
	})
	if err != nil {
		return err
	}

	d.lock.Lock()
	err = d.loadClear(r)
	if err != nil {
		return err
	}
	d.lock.Unlock()

	err = d.loadConfig()
	if err != nil {
		return err
	}

	for _, col := range d.collections {
		col.db = d
		for _, index := range col.BleveIndexes {
			index.collection = col
			err = index.indexUnzipper()
			if err != nil {
				return err
			}
		}
	}

	return d.loadCollections()
}

func (d *DB) goRoutineLoopForGC() {
	ticker := time.NewTicker(time.Minute * 15)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// d.badger.Flatten(runtime.NumCPU())
			d.badger.RunValueLogGC(0.5)
		case <-d.ctx.Done():
			return
		}
	}
}

// This is where all writes are made
func (d *DB) goRoutineLoopForWrites() {
	limitNumbersOfWriteOperation := 10000
	limitSizeOfWriteOperation := 100 * 1000 * 1000 // 100MB
	limitWaitBeforeWriteStart := time.Millisecond * 50

	d.lock.RLock()
	badgerStore := d.badger
	localCtx := d.ctx
	writeChan := d.writeChan
	d.lock.RUnlock()

	for {
		writeSizeCounter := 0

		var trans *transaction.Transaction
		var ok bool
		select {
		case trans, ok = <-writeChan:
			if !ok {
				return
			}
		case <-localCtx.Done():
			return
		}

		// Save the size of the write
		writeSizeCounter += trans.GetWriteSize()
		firstArrivedAt := time.Now()

		// Add to the list of operation to be done
		waitingWrites := []*transaction.Transaction{trans}

		// Try to empty the queue if any
	tryToGetAnOtherRequest:
		select {
		// There is an other request in the queue
		case nextWrite := <-writeChan:
			// And save the response channel
			waitingWrites = append(waitingWrites, nextWrite)

			// Check if the limit is not reach
			if len(waitingWrites) < limitNumbersOfWriteOperation &&
				writeSizeCounter < limitSizeOfWriteOperation &&
				time.Since(firstArrivedAt) < limitWaitBeforeWriteStart {
				// If not lets try to empty the queue a bit more
				goto tryToGetAnOtherRequest
			}
			// This continue if there is no more request in the queue
		case <-localCtx.Done():
			return
			// Stop waiting and do present operations
		default:
		}

		err := badgerStore.Update(func(txn *badger.Txn) error {
			for _, transaction := range waitingWrites {
				for _, op := range transaction.Operations {
					var err error
					if op.Delete {
						err = txn.Delete(op.DBKey)
					} else {
						if op.CleanHistory {
							entry := badger.NewEntry(op.DBKey, cipher.Encrypt(d.privateKey, op.DBKey, op.Value))
							entry.WithDiscard()
							err = txn.SetEntry(entry)
						} else {
							err = txn.Set(op.DBKey, cipher.Encrypt(d.privateKey, op.DBKey, op.Value))
						}
					}

					// Returns the write error to the caller
					if err != nil {
						go d.nonBlockingResponseChan(localCtx, transaction, err)
					}

				}
			}
			return nil
		})

		// Dispatch the commit response to all callers
		for _, op := range waitingWrites {
			go d.nonBlockingResponseChan(localCtx, op, err)
		}
	}
}

func (d *DB) nonBlockingResponseChan(ctx context.Context, tx *transaction.Transaction, err error) {
	// d.lock.RLock()
	// localCtx := d.ctx
	// d.lock.RUnlock()

	select {
	case tx.ResponseChan <- err:
	case <-ctx.Done():
	case <-tx.Ctx.Done():
	}
}

func (d *DB) decryptData(dbKey, encryptedData []byte) (clear []byte, err error) {
	return cipher.Decrypt(d.privateKey, dbKey, encryptedData)
}

// saveConfig save the database configuration with collections and indexes
func (d *DB) saveConfig() (err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	return d.badger.Update(func(txn *badger.Txn) error {
		// Convert to JSON
		dbToSaveAsBytes, err := json.Marshal(d)
		if err != nil {
			return err
		}

		dbKey := []byte{prefixConfig}
		e := &badger.Entry{
			Key:   dbKey,
			Value: cipher.Encrypt(d.configKey, dbKey, dbToSaveAsBytes),
		}

		return txn.SetEntry(e)
	})
}

func (d *DB) getConfigValue() (config *dbExport, err error) {
	var dbAsBytes []byte

	d.lock.RLock()
	defer d.lock.RUnlock()

	err = d.badger.View(func(txn *badger.Txn) error {
		dbKey := []byte{prefixConfig}

		var item *badger.Item
		item, err = txn.Get(dbKey)
		if err != nil {
			return err
		}

		dbAsBytes, err = item.ValueCopy(dbAsBytes)
		if err != nil {
			return err
		}

		dbAsBytes, err = cipher.Decrypt(d.configKey, dbKey, dbAsBytes)
		if err != nil {
			return err
		}

		return nil
	})

	config = new(dbExport)

	if err != nil {
		if err == badger.ErrKeyNotFound {
			n, err := rand.Read(config.PrivateKey[:])
			if err != nil {
				return nil, err
			}
			if n != 32 {
				return nil, fmt.Errorf("generate internal key is %d length instead of %d", n, 32)
			}

			return config, nil
		}
		return nil, err
	}

	err = json.Unmarshal(dbAsBytes, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (d *DB) getConfig() (db *dbExport, err error) {
	db, err = d.getConfigValue()
	if err != nil {
		return nil, err
	}

	return
}

func (d *DB) loadConfig() error {
	db, err := d.getConfig()
	if err != nil {
		return err
	}

	d.lock.Lock()
	d.collections = db.Collections
	d.privateKey = db.PrivateKey
	d.lock.Unlock()

	// d.cancel()

	// db.cancel = d.cancel
	// db.badger = d.badger
	// db.ctx, db.cancel = context.WithCancel(context.Background())
	// db.writeChan = d.writeChan
	// db.path = d.path
	// db.FileStore = d.FileStore
	// db.lock = d.lock
	// d.lock.Lock()

	// *d = *db
	// d.lock.Unlock()

	d.startBackgroundLoops()

	return nil
}

func (d *DB) loadCollections() (err error) {
	for _, col := range d.collections {
		for _, index := range col.BleveIndexes {
			index.collection = col
			indexPrefix := make([]byte, len(index.prefix))
			copy(indexPrefix, index.prefix)
			config := blevestore.NewConfigMap(d.ctx, index.Path, d.privateKey, indexPrefix, d.badger, d.writeChan)
			index.bleveIndex, err = bleve.OpenUsing(d.path+string(os.PathSeparator)+index.Path, config)
			if err != nil {
				// if index.bleveIndex == nil {
				// 	return
				// }
				return
			}
		}
	}

	return nil
}

// DeleteCollection removes every document and indexes and the collection itself
func (d *DB) DeleteCollection(colName string) {
	var col *Collection
	for i, tmpCol := range d.collections {
		if tmpCol.name == colName {
			col = tmpCol

			copy(d.collections[i:], d.collections[i+1:])
			d.collections[len(d.collections)-1] = nil // or the zero value of T
			d.collections = d.collections[:len(d.collections)-1]

			break
		}
	}

	for _, index := range col.BleveIndexes {
		col.DeleteIndex(index.name)
	}

	d.deletePrefix(col.prefix)
}

func (d *DB) deletePrefix(prefix []byte) error {
	return d.badger.DropPrefix(prefix)
}
