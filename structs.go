package gotinydb

import (
	"context"
	"os"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/google/btree"
)

type (
	// DB is the main element of the package and provide all access to sub commands
	DB struct {
		options *Options

		badgerDB    *badger.DB
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

		// CryptoKey if present must be 32 bytes long
		CryptoKey []byte

		FileChunkSize int

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

	// Query defines the object to request index query.
	Query struct {
		filters []*Filter

		orderSelector []string
		order         uint64 // is the selector hash representation
		ascendent     bool   // defines the way of the order

		limit         int
		internalLimit int
		timeout       time.Duration
	}

	// idType is a type to order IDs during query to be compatible with the tree query
	idType struct {
		ID          string
		occurrences int
		ch          chan int
		// values defines the different values and selector that called this ID
		// selectors are defined by a hash 64
		values map[uint64][]byte

		// This is for the ordering
		less         func(btree.Item) bool
		selectorHash uint64
		getRefsFunc  func(id string) *refs
	}

	// idsType defines a list of ID. The struct is needed to build a pointer to be
	// passed to deferent functions
	idsType struct {
		IDs []*idType
	}

	idsTypeMultiSorter struct {
		IDs    []*idType
		invert bool
	}

	// FilterOperator defines the type of filter to perform
	filterOperator string

	// Response holds the results of a query
	Response struct {
		list           []*ResponseElem
		actualPosition int
		query          *Query
	}

	// ResponseElem defines the response as a pointer
	ResponseElem struct {
		_ID            *idType
		contentAsBytes []byte
	}

	// Filter defines the way the query will be performed
	Filter struct {
		selector     []string
		selectorHash uint64
		operator     filterOperator
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
		id                  string
		collection          *Collection
		contentInterface    interface{}
		contentAsBytes      []byte
		chunkN              int
		bin                 bool
		isInsertion, isFile bool
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
