package gotinydb

import (
	"context"
	"os"
	"time"

	"github.com/dgraph-io/badger"
)

type (
	// DB is the main element of the package and provide all access to sub commands
	DB struct {
		options *Options

		valueStore  *badger.DB
		collections []*Collection

		// freePrefix defines the list of prefix which can be used for a new collection
		freePrefix []byte

		writeTransactionChan chan *writeTransaction

		ctx     context.Context
		closing bool
	}

	dbExport struct {
		Collections []*collectionExport
		FreePrefix  []byte
	}
	collectionExport struct {
		Name    string
		Indexes []*indexType
		Prefix  byte
	}

	// Options defines the deferent configuration elements of the database
	Options struct {
		Path                             string
		TransactionTimeOut, QueryTimeOut time.Duration
		InternalQueryLimit               int
		// This define the limit which apply to the serialization of the writes
		PutBufferLimit int

		BadgerOptions *badger.Options
	}

	// Collection defines the storage object
	Collection struct {
		name    string
		indexes []*indexType

		// prefix defines the prefix needed to found the collection into the store
		prefix byte

		options *Options

		store *badger.DB

		writeTransactionChan chan *writeTransaction

		ctx context.Context

		saveCollections func() error
	}

	// Filter defines the way the query will be performed
	Filter interface {
		// GetType returns the type of the filter given at the initialization
		GetType() FilterOperator
		// ExclusionFilter set the given Filter to be used as a cleaner filter.
		// When IDs are retrieved by those filters the IDs will not be returned at response.
		ExclusionFilter() Filter

		getFilterBase() *filterBase
	}

	filterBase struct {
		selector     []string
		selectorHash uint64
		operator     FilterOperator
		values       []*filterValue
		exclusion    bool
	}

	// IndexType defines what kind of field the index is scanning
	IndexType int

	// filterValue defines the value we need to compare to
	filterValue struct {
		Value interface{}
		Type  IndexType
	}

	// Index defines the struct to manage indexation
	indexType struct {
		Name     string
		Selector []string
		Type     IndexType

		options *Options

		getTx        func(update bool) *badger.Txn
		getIDBuilder func(id []byte) []byte
	}

	// refs defines an struct to manage the references of a given object
	// in all the indexes it belongs to
	refs struct {
		ObjectID string
		// ObjectHashID string

		Refs []*ref
	}

	// ref defines the relations between a object with some index with indexed value
	ref struct {
		IndexName    string
		IndexHash    uint64
		IndexedValue []byte
	}

	writeTransaction struct {
		responseChan chan error
		ctx          context.Context
		transactions []*writeTransactionElement
	}
	writeTransactionElement struct {
		id               string
		collection       *Collection
		contentInterface interface{}
		contentAsBytes   []byte
		bin              bool
		isInsertion      bool
	}

	// Archive defines the way archives are saved inside the zip file
	archive struct {
		StartTime, EndTime time.Time
		Indexes            map[string][]*indexType
		Collections        []string
		Timestamp          uint64

		file *os.File
	}

	// IndexInfo is returned by *Collection.GetIndexesInfo and let call see
	// what indexes are present in the collection.
	IndexInfo struct {
		Name     string
		Selector []string
		Type     IndexType
	}
)
