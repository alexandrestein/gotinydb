package gotinydb

import "fmt"

// Those constants defines the first level of prefixes.
const (
	prefixConfig byte = iota
	prefixCollections
	prefixFiles
	prefixFilesRelated
	prefixTTL
)

// Those constants defines the second level of prefixes or value from config.
const (
	prefixCollectionsData byte = iota
	prefixCollectionsBleveIndex
)

// This defines most of the package errors
var (
	ErrNotFound           = fmt.Errorf("not found")
	ErrHashCollision      = fmt.Errorf("the name is in collision with an other element")
	ErrEmptyID            = fmt.Errorf("ID must be provided")
	ErrIndexNotFound      = fmt.Errorf("index not found")
	ErrNameAllreadyExists = fmt.Errorf("element with the same name allready exists")
	ErrGetMultiNotEqual   = fmt.Errorf("you must provied the same number of ids and destinations")

	ErrEndOfQueryResult = fmt.Errorf("there is no more values to retrieve from the query")

	ErrFileInWrite              = fmt.Errorf("this file is already in write mode")
	ErrFileItemIteratorNotValid = fmt.Errorf("item is not valid")
)

var (
	// FileChuckSize define the default chunk size when saving files
	fileChuckSize = 5 * 1000 * 1000 // 5MB
)
