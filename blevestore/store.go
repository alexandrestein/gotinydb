package blevestore

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger/v2"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	"github.com/alexandrestein/gotinydb/transaction"
)

const (
	// Name defines the internal name given to the bleve store
	Name = "gotinydb"
)

type (
	// Config defines the different configurations needed to make the store work
	Config struct {
		ctx                 context.Context
		key                 [32]byte
		prefix              []byte
		db                  *badger.DB
		writesChan          chan *transaction.Transaction
		transactionsTimeOut time.Duration
	}

	// Store implements the blevestore interface
	Store struct {
		// name is defined by the path
		name   string
		config *Config
		mo     store.MergeOperator
	}
)

// New returns a new store with the given options
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	if path == "" {
		return nil, os.ErrInvalid
	}

	configPointer, ok := config["config"].(*Config)
	if !ok {
		return nil, fmt.Errorf("must specify the config")
	}

	rv := Store{
		name:   path,
		config: configPointer,
		mo:     mo,
	}
	return &rv, nil
}

// Close is self explained
func (bs *Store) Close() error {
	return nil
}

// Reader open a new transaction but it needs to be closed
func (bs *Store) Reader() (store.KVReader, error) {
	// iterators := []*badger.Iterator{}
	return &Reader{
		store: bs,
		txn:   bs.config.db.NewTransaction(false),
		// indexPrefixID: bs.config.prefix,
		// iterators:     iterators,
	}, nil
}

// Writer returns a new writer interface
func (bs *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: bs,
	}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}

func (bs *Store) buildID(key []byte) []byte {
	prefix := make([]byte, len(bs.config.prefix))
	copy(prefix, bs.config.prefix)
	dbKey := append(prefix, key...)
	return dbKey
}

// NewConfig returns the configuration as an pointer
func NewConfig(ctx context.Context, key [32]byte, prefix []byte, db *badger.DB, writeElementsChan chan *transaction.Transaction) (config *Config) {
	return &Config{
		ctx:        ctx,
		key:        key,
		prefix:     prefix,
		db:         db,
		writesChan: writeElementsChan,
	}
}

// NewConfigMap returns the configuration as a map
func NewConfigMap(ctx context.Context, path string, key [32]byte, prefix []byte, db *badger.DB, writeElementsChan chan *transaction.Transaction) map[string]interface{} {
	return map[string]interface{}{
		"path": path,
		"config": NewConfig(
			ctx,
			key,
			prefix,
			db,
			writeElementsChan,
		),
	}
}
