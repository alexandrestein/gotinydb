package gotinydb

import (
	"context"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"

	"github.com/alexandrestein/gotinydb/transactions"
)

type (
	// DB is the main element of the package and provide all access to sub commands
	DB struct {
		options *Options

		badgerDB    *badger.DB
		collections []*Collection

		// freeCollectionPrefixes defines the list of prefix which can be used for a new collection
		freeCollectionPrefixes []byte

		writeTransactionChan chan *transactions.WriteTransaction

		ctx     context.Context
		closing bool
	}

	dbExport struct {
		Collections            []*collectionExport
		FreeCollectionPrefixes []byte
		PrivateCryptoKey       [32]byte
	}
	collectionExport struct {
		Name         string
		BleveIndexes []*bleveIndex
		Prefix       byte
	}

	// Options defines the deferent configuration elements of the database
	Options struct {
		Path                             string
		TransactionTimeOut, QueryTimeOut time.Duration
		InternalQueryLimit               int
		// This define the limit which apply to the serialization of the writes
		PutBufferLimit int

		// CryptoKey if present must be 32 bytes long, Otherwise an empty key is used.
		CryptoKey [32]byte
		// privateCryptoKey is saved on the database to provide a way to change the password
		// without the need to rewrite the all database
		privateCryptoKey [32]byte

		// GCCycle define the time the loop for garbage collection takes to run the GC.
		GCCycle time.Duration

		FileChunkSize int

		BadgerOptions *badger.Options
	}

	// Collection defines the storage object
	Collection struct {
		name string
		// indexes      []*indexType
		bleveIndexes []*bleveIndex

		// prefix defines the prefix needed to found the collection into the store
		prefix byte

		options *Options

		store *badger.DB

		writeTransactionChan chan *transactions.WriteTransaction

		ctx context.Context

		saveCollections func() error
	}

	bleveIndex struct {
		Name        string
		Path        string
		IndexDirZip []byte
		IndexPrefix []byte
		Selector    selector

		kvConfig map[string]interface{}
		writeTxn *badger.Txn

		index bleve.Index
	}

	// SearchResult is returned when (*Collection).Shearch is call.
	// It contains the result and a iterator for the reading values directly from database.
	SearchResult struct {
		BleveSearchResult *bleve.SearchResult

		position uint64
		c        *Collection
	}

	// Response defines the response as a pointer
	Response struct {
		// _ID            *idType
		ID             string
		ContentAsBytes []byte
	}

	// selector is used to manage the index field selection during insertion or update
	selector []string
)
