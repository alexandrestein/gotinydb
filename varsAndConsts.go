package gotinydb

import (
	"fmt"
	"os"
	"time"
)

// Defines the default values of the database configuration
var (
	DefaultTransactionTimeOut = time.Second
	DefaultQueryTimeOut       = time.Second * 5
	DefaultQueryLimit         = 100
	DefaultInternalQueryLimit = 1000
)

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

	// ErrTheResponseIsOver defines error when *ResponseQuery.One is called and all response has been returned
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
