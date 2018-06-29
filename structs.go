package gotinydb

import (
	"context"
	"time"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

type (
	// DB is the main element of the package and provide all access to sub commands
	DB struct {
		path string
		conf *Conf

		valueStore  *badger.DB
		collections []*Collection

		ctx     context.Context
		closing bool
	}

	// Conf defines the deferent configuration elements of the database
	Conf struct {
		TransactionTimeOut, QueryTimeOut time.Duration
		InternalQueryLimit               int
	}

	// Collection defines the storage object
	Collection struct {
		name, id string
		indexes  []*indexType

		conf *Conf

		db    *bolt.DB
		store *badger.DB

		writeTransactionChan chan *writeTransaction

		ctx context.Context
	}

	// Filter defines the way the query will be performed
	Filter struct {
		selector     []string
		selectorHash uint64
		operator     FilterOperator
		values       []*filterValue
		equal        bool
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
		Name         string
		Selector     []string
		SelectorHash uint64
		Type         IndexType

		conf *Conf

		getTx func(update bool) (*bolt.Tx, error)
	}

	// refs defines an struct to manage the references of a given object
	// in all the indexes it belongs to
	refs struct {
		ObjectID     string
		ObjectHashID string

		Refs []*ref
	}

	// Ref defines the relations between a object with some index with indexed value
	ref struct {
		IndexName    string
		IndexHash    uint64
		IndexedValue []byte
	}

	writeTransaction struct {
		id               string
		contentInterface interface{}
		contentAsBytes   []byte
		responseChan     chan error
		ctx              context.Context
		bin              bool
	}
)
