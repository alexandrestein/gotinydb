package gotinydb

import (
	bolt "github.com/coreos/bbolt"
	"github.com/dgraph-io/badger"
)

type (
	Collection struct {
		Name string

		DB    *bolt.DB
		Store *badger.DB
	}
)

func (c *Collection) Put() error {

}

func ()  {

}
