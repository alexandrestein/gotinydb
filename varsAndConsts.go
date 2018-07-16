package gotinydb

import (
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

// Defines the default values of the database configuration
var (
	DefaultTransactionTimeOut = time.Second
	DefaultQueryTimeOut       = time.Second * 5
	DefaultQueryLimit         = 100
	DefaultInternalQueryLimit = 1000

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
		NumVersionsToKeep: 0,

		ValueLogFileSize:   1 << 30,
		ValueLogMaxEntries: 1000000,
		ValueThreshold:     32,
		Truncate:           false,
	}

	DefaultBoltOptions = bolt.DefaultOptions
)

// NewDefaultOptions build default options with a path
func NewDefaultOptions(path string) *Options {
	return &Options{
		Path:               path,
		TransactionTimeOut: DefaultTransactionTimeOut,
		QueryTimeOut:       DefaultQueryTimeOut,
		InternalQueryLimit: DefaultQueryLimit,

		BadgerOptions: DefaultBadgerOptions,
		BoltOptions:   DefaultBoltOptions,
	}
}

var (
	// FilePermission defines the database file permission
	FilePermission os.FileMode = 0740 // u -> rwx | g -> r-- | o -> ---

	// ErrWrongType defines the wrong type error
	ErrWrongType = fmt.Errorf("wrong type")
	// ErrNotFound defines error when the asked ID is not found
	ErrNotFound = fmt.Errorf("not found")
	// ErrEmptyID defines error when the given id is empty
	ErrEmptyID = fmt.Errorf("empty ID")
	// ErrTimeOut defines the error when the query is timed out
	ErrTimeOut = fmt.Errorf("timed out")

	// ErrTheResponseIsOver defines error when *Response.One is called and all response has been returned
	ErrTheResponseIsOver = fmt.Errorf("the response has no more values")
)

// Those constants defines the different types of filter to perform at query
const (
	Equal   FilterOperator = "eq"
	Greater FilterOperator = "gr"
	Less    FilterOperator = "le"
	Between FilterOperator = "bw"
)

// Those define the different type of indexes
const (
	StringIndex IndexType = iota
	IntIndex
	TimeIndex
)
