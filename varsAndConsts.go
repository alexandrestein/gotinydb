package gotinydb

import (
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

// Defines the default values of the database configuration
var (
	// Used to get the configuration after restarting the database
	configID = []byte{prefixConfig, dbIDPrefixConfigDBConfig}

	DefaultTransactionTimeOut = time.Second * 1
	DefaultQueryTimeOut       = time.Second * 1
	DefaultGCCycle            = time.Minute * 1
	DefaultQueryLimit         = 100
	DefaultInternalQueryLimit = 1000
	DefaultPutBufferLimit     = 100

	// Default file chunk size is 10MB
	DefaltFileChunkSize = 10 * 1000 * 1000

	// FilePermission defines the database file permission
	FilePermission os.FileMode = 0740 // u -> rwx | g -> r-- | o -> ---

	DefaultBadgerOptions = &badger.Options{
		DoNotCompact:        false,
		LevelOneSize:        256 << 20,
		LevelSizeMultiplier: 10,
		TableLoadingMode:    options.LoadToRAM,
		ValueLogLoadingMode: options.MemoryMap,

		MaxLevels:               7,
		MaxTableSize:            64 << 20,
		NumCompactors:           3,
		NumLevelZeroTables:      5,
		NumLevelZeroTablesStall: 10,
		NumMemtables:            5,
		SyncWrites:              true,
		// NumVersionsToKeep:       1,
		NumVersionsToKeep: 10,

		ValueLogFileSize:   1 << 30,
		ValueLogMaxEntries: 1000000,
		ValueThreshold:     32,
		Truncate:           false,
	}
)

// NewDefaultOptions build default options with a path
func NewDefaultOptions(path string) *Options {
	return &Options{
		Path:               path,
		TransactionTimeOut: DefaultTransactionTimeOut,
		QueryTimeOut:       DefaultQueryTimeOut,
		InternalQueryLimit: DefaultQueryLimit,
		PutBufferLimit:     DefaultPutBufferLimit,

		GCCycle: DefaultGCCycle,

		BadgerOptions: DefaultBadgerOptions,
	}
}

// Defines the errors
var (
	// ErrBadBadgerConfig is returned when opening the database and the issue is from the Badger configuration
	ErrBadBadgerConfig = fmt.Errorf("Badger configuration is not valid")
	// ErrRollbackVersionNotFound is returned when rollback is requested but the target value can't be found
	ErrRollbackVersionNotFound = fmt.Errorf("passed to an other key before hitting the requested version")
	// ErrClosedDB is returned when the database is closed but a call has been run
	ErrClosedDB = fmt.Errorf("data base is closed or on it's way to close")
	// ErrPutMultiWrongLen is returned when calling a multiple put instruction but the IDs and the content don't have the same length
	ErrPutMultiWrongLen = fmt.Errorf("the IDs and content lists must have the same length")
	// ErrWrongType defines the wrong type error
	ErrWrongType = fmt.Errorf("wrong type")
	// ErrNotFound defines error when the asked ID is not found
	ErrNotFound = fmt.Errorf("not found")
	// ErrIndexNotFound is returned when no index match the query
	ErrIndexNotFound = fmt.Errorf("index not found")
	// ErrIndexNameAllreadyExists is returned when try to add an index but the same name is present in the list of indexes
	ErrIndexNameAllreadyExists = fmt.Errorf("index with same name exists")
	// ErrIndexLimit is returned when the caller exceed the limit of indexes which is 256 indexes per collection
	ErrIndexLimit = fmt.Errorf("there is no left prefix for this index")
	// ErrSearchOver is returned when *SearchResult.Next is called but there is no more ID to retrieve
	ErrSearchOver = fmt.Errorf("results done")

	// ErrEmptyID defines error when the given id is empty
	ErrEmptyID = fmt.Errorf("empty ID")
	// ErrTimeOut defines the error when the query is timed out
	ErrTimeOut = fmt.Errorf("timed out")
	// ErrDataCorrupted defines the error when the checksum is not valid
	ErrDataCorrupted = fmt.Errorf("content corrupted")
	// ErrResponseOver defines error when *Response.One is called and all response has been returned
	ErrResponseOver = fmt.Errorf("the response has no more values")
)

// // Those constants defines the different types of filter to perform at query
// const (
// 	equal    filterOperator = "eq"
// 	greater  filterOperator = "gr"
// 	less     filterOperator = "le"
// 	between  filterOperator = "bw"
// 	exists   filterOperator = "ex"
// 	contains filterOperator = "cn"
// )

// Those constants defines the first level of prefixes.
const (
	prefixConfig byte = iota
	prefixCollections
	prefixFiles
)

// Those constants defines the second level of prefixes or value from config.
const (
	dbIDPrefixConfigDBConfig byte = iota
)

// Those constants defines the prefix used to split different element of the collection
// into the store. This is the second level of the collection prefix.
const (
	prefixData byte = iota
	// prefixIndexes
	// prefixRefs
	prefixBleveIndexes
)

// // Those define the different type of indexes
// const (
// 	StringIndex IndexType = iota
// 	IntIndex
// 	UIntIndex
// 	TimeIndex

// 	StringIndexString filterOperator = "string"
// 	IntIndexString    filterOperator = "int"
// 	UIntIndexString   filterOperator = "uint"
// 	TimeIndexString   filterOperator = "time"
// )
