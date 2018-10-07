package blevestore

import (
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	"github.com/alexandrestein/gotinydb/transaction"
)

const (
	Name = "internal"
)

type (
	BleveStoreConfig struct {
		key                 [32]byte
		prefix              []byte
		db                  *badger.DB
		writesChan          chan *transaction.Transaction
		transactionsTimeOut time.Duration
	}

	Store struct {
		// name is defined by the path
		name   string
		config *BleveStoreConfig
		// writeTxn             *badger.Txn
		// primaryEncryptionKey *[32]byte
		// indexPrefixID        []byte
		// indexPrefixIDLen     int
		// db *badger.DB
		mo store.MergeOperator
	}
)

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	if path == "" {
		return nil, os.ErrInvalid
	}

	configPointer, ok := config["config"].(*BleveStoreConfig)
	if !ok {
		return nil, fmt.Errorf("must specify the config")
	}

	// prefixID, ok := config["prefix"].([]byte)
	// if !ok {
	// 	return nil, fmt.Errorf("must specify a prefix")
	// }

	// db, ok := config["db"].(*badger.DB)
	// if !ok {
	// 	return nil, fmt.Errorf("must specify a db")
	// }

	// primaryEncryptionKey, ok := config["key"].(*[32]byte)
	// if !ok {
	// 	return nil, fmt.Errorf("must specify a key as [32]byte")
	// }

	// encrypt, ok := config["encrypt"].(func(dbID, clearContent []byte) []byte)
	// if !ok {
	// 	return nil, fmt.Errorf("the encrypt function must be provided")
	// }

	// decrypt, ok := config["decrypt"].(func(dbID, encryptedContent []byte) (decryptedContent []byte, _ error))
	// if !ok {
	// 	return nil, fmt.Errorf("the decrypt function must be provided")
	// }

	// writeTxn, ok := config["writeTxn"].(*badger.Txn)
	// if !ok {
	// 	return nil, fmt.Errorf("the write transaction pointer must be initialized")
	// }

	rv := Store{
		name:   path,
		config: configPointer,
		mo:     mo,
	}
	return &rv, nil
}

func (bs *Store) Close() error {
	return nil
}

// Reader open a new transaction but it needs to be closed
func (bs *Store) Reader() (store.KVReader, error) {
	// iterators := []*badger.Iterator{}
	return &Reader{
		store:         bs,
		txn:           bs.config.db.NewTransaction(false),
		indexPrefixID: bs.config.prefix,
		// iterators:     iterators,
	}, nil
}

func (bs *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: bs,
	}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}

func (bs *Store) buildID(key []byte) []byte {
	return append(bs.config.prefix, key...)
}

func NewBleveStoreConfig(key [32]byte, prefix []byte, db *badger.DB, writeElementsChan chan *transaction.Transaction) (config *BleveStoreConfig) {
	return &BleveStoreConfig{
		key:        key,
		prefix:     prefix,
		db:         db,
		writesChan: writeElementsChan,
	}
}

func NewBleveStoreConfigMap(path string, key [32]byte, prefix []byte, db *badger.DB, writeElementsChan chan *transaction.Transaction) map[string]interface{} {
	return map[string]interface{}{
		"path": path,
		"config": NewBleveStoreConfig(
			key,
			prefix,
			db,
			writeElementsChan,
		),
	}
}

// func NewIndexRequest(ctx context.Context, id string, data interface{}) *IndexRequest {
// 	writesChan := make(chan []*transactions.WriteElement, 0)

// 	return &IndexRequest{
// 		ID:               id,
// 		Data:             data,
// 		WriteOpperations: writesChan,
// 	}
// }

// func NewBleveStoreWriteRequest(requests []*transactions.WriteElement) *transactions.WriteTransaction {
// 	ret := new(transactions.WriteTransaction)
// 	ret.ResponseChan = make(chan error, 0)
// 	ret.Transactions = requests

// 	return ret
// }
