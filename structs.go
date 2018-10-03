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
		bleveIndexChan chan 
		// writeBleveIndexChan  chan *blevestore.BleveStoreWriteRequest

		ctx     context.Context
		closing bool
	}

	dbExport struct {
		Collections            []*collectionExport
		FreeCollectionPrefixes []byte
		PrivateCryptoKey       [32]byte
	}
	collectionExport struct {
		Name string
		// Indexes      []*indexType
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
		// writeBleveIndexChan  chan *blevestore.BleveStoreWriteRequest

		ctx context.Context

		saveCollections func() error
	}

	// // Query defines the object to request index query.
	// Query struct {
	// 	filters []*Filter

	// 	orderSelector selector
	// 	order         uint16 // is the selector hash representation
	// 	ascendent     bool   // defines the way of the order

	// 	limit         int
	// 	internalLimit int
	// 	timeout       time.Duration
	// }

	// // idType is a type to order IDs during query to be compatible with the tree query
	// idType struct {
	// 	ID          string
	// 	occurrences int
	// 	ch          chan int
	// 	// values defines the different values and selector that called this ID
	// 	// selectors are defined by a hash 64
	// 	values map[uint16][]byte

	// 	// This is for the ordering
	// 	less         func(btree.Item) bool
	// 	selectorHash uint16
	// 	getRefsFunc  func(id string) *refs
	// }

	// // idsType defines a list of ID. The struct is needed to build a pointer to be
	// // passed to deferent functions
	// idsType struct {
	// 	IDs []*idType
	// }

	// idsTypeMultiSorter struct {
	// 	IDs    []*idType
	// 	invert bool
	// }

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

	// BleveSearchResult is returned when (*Collection).Shearch is call.
	// It contains the result and a iterator for the reading values directly from database.
	BleveSearchResult struct {
		BleveSearchResult *bleve.SearchResult

		position uint64
		c        *Collection

		// preload      uint
		// preloaded    [][]byte
		// preloadedErr []error
	}

	// SearchResult is returned when (*Collection).Shearch is call.
	// It contains the result and a iterator for the reading values directly from database.
	SearchResult struct {
		BleveSearchResult *bleve.SearchResult

		position uint64
		c        *Collection

		// preload      uint
		// preloaded    [][]byte
		// preloadedErr []error
	}

	// // FilterOperator defines the type of filter to perform
	// filterOperator string

	// Response holds the results of a query
	Response struct {
		list           []*ResponseElem
		actualPosition int
		// query          *Query
	}

	// ResponseElem defines the response as a pointer
	ResponseElem struct {
		// _ID            *idType
		ID             string
		ContentAsBytes []byte
	}

	// // Filter defines the way the query will be performed
	// Filter struct {
	// 	selector     selector
	// 	selectorHash uint16
	// 	operator     filterOperator
	// 	values       []*filterValue
	// 	exclusion    bool
	// }

	// IndexType defines what kind of field the index is scanning
	IndexType int

	// // filterValue defines the value we need to compare to
	// filterValue struct {
	// 	Value interface{}
	// 	Type  IndexType
	// }

	// // Index defines the struct to manage indexation
	// indexType struct {
	// 	Name     string
	// 	Selector selector
	// 	Type     IndexType

	// 	options *Options

	// 	getTx        func(update bool) *badger.Txn
	// 	getIDBuilder func(id []byte) []byte
	// }

	// // refs defines an struct to manage the references of a given object
	// // in all the indexes it belongs to
	// refs struct {
	// 	ObjectID string
	// 	// ObjectHashID string

	// 	Refs []*ref
	// }

	// // ref defines the relations between a object with some index with indexed value
	// ref struct {
	// 	IndexName    string
	// 	IndexHash    uint16
	// 	IndexedValue []byte
	// }

	// writeTransaction struct {
	// 	responseChan chan error
	// 	ctx          context.Context
	// 	transactions []*transactions.WriteElement
	// }
	// WriteTransactionElement struct {
	// 	// id                  string
	// 	// collection          *Collection
	// 	// contentInterface    interface{}
	// 	// chunkN              int
	// 	// bin                 bool
	// 	// isInsertion, isFile bool
	// 	// bleveIndex          bool

	// 	DBKey          []byte
	// 	ContentAsBytes []byte
	// }

	// // Archive defines the way archives are saved inside the zip file
	// archive struct {
	// 	StartTime, EndTime time.Time
	// 	Indexes            map[string][]*indexType
	// 	Collections        []string
	// 	Timestamp          uint64

	// 	file *os.File
	// }

	// // IndexInfo is returned by *Collection.GetIndexesInfo and let call see
	// // what indexes are present in the collection.
	// IndexInfo struct {
	// 	Name     string
	// 	Selector selector
	// 	Type     IndexType
	// }

	selector []string
)
