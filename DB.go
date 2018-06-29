package gotinydb

import (
	"context"
	"fmt"
	"os"
	"time"

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
)

// Defines the default values of the database configuration
var (
	DefaultTransactionTimeOut = time.Second
	DefaultQueryTimeOut       = time.Second * 5
	DefaultQueryLimit         = 100
	DefaultInternalQueryLimit = 1000
)

// Open simply opens a new or existing database
func Open(ctx context.Context, path string) (*DB, error) {
	d := new(DB)
	d.path = path
	d.ctx = ctx

	d.conf = &Conf{DefaultTransactionTimeOut, DefaultQueryTimeOut, DefaultInternalQueryLimit}

	if err := d.buildPath(); err != nil {
		return nil, err
	}

	if initBadgerErr := d.initBadger(); initBadgerErr != nil {
		return nil, initBadgerErr
	}

	if loadErr := d.loadCollections(); loadErr != nil {
		return nil, loadErr
	}

	go d.waitForClose()

	return d, nil
}

// Use build or get a Collection pointer
func (d *DB) Use(colName string) (*Collection, error) {
	for _, col := range d.collections {
		if col.name == colName {
			if err := col.loadIndex(); err != nil {
				return nil, err
			}
			return col, nil
		}
	}

	c, loadErr := d.getCollection("", colName)
	if loadErr != nil {
		return nil, loadErr
	}

	if err := c.loadIndex(); err != nil {
		return nil, err
	}
	d.collections = append(d.collections, c)

	return c, nil
}

// SetConfig update the database configurations
func (d *DB) SetConfig(conf *Conf) error {
	d.conf = conf

	for _, col := range d.collections {
		col.conf = conf
		for _, index := range col.indexes {
			index.conf = conf
		}
	}
	return nil
}

// Close close the underneath collections and main store
func (d *DB) Close() error {
	if d.closing {
		return fmt.Errorf("already ongoing")
	}
	d.closing = true

	errors := ""
	for i, col := range d.collections {
		if err := col.db.Close(); err != nil {
			errors = fmt.Sprintf("%s%s\n", errors, err.Error())
		}
		d.collections[i] = nil
	}

	if d.valueStore != nil {
		err := d.valueStore.Close()
		if err != nil {
			errors = fmt.Sprintf("%s%s\n", errors, err.Error())
		}
	}

	if errors != "" {
		return fmt.Errorf(errors)
	}

	d.path = ""
	d.valueStore = nil
	d.collections = nil

	d = nil
	return nil
}

// DeleteCollection delete the given collection
func (d *DB) DeleteCollection(collectionName string) error {
	var c *Collection
	for i, col := range d.collections {
		if col.name == collectionName {
			// Save the collection pointer for future cleanup
			c = col
			// Delete the collection form the list of collection pointers
			copy(d.collections[i:], d.collections[i+1:])
			d.collections[len(d.collections)-1] = nil
			d.collections = d.collections[:len(d.collections)-1]
			break
		}
	}

	// Close index DB
	if err := c.db.Close(); err != nil {
		return err
	}
	// Remove the index DB files
	if err := os.RemoveAll(d.path + "/collections/" + c.id); err != nil {
		return err
	}

	// Remove stored values 1000 by 1000
	for {
		ids, err := c.getAllStoreIDs(1000)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}

		err = d.valueStore.Update(func(txn *badger.Txn) error {
			for _, id := range ids {
				err := txn.Delete(id)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
}
