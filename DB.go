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
		Path string
		Conf *Conf

		ValueStore  *badger.DB
		Collections []*Collection

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
	d.Path = path
	d.ctx = ctx

	d.Conf = &Conf{DefaultTransactionTimeOut, DefaultQueryTimeOut, DefaultInternalQueryLimit}

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
	for _, col := range d.Collections {
		if col.Name == colName {
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
	d.Collections = append(d.Collections, c)

	return c, nil
}

// SetConfig update the database configurations
func (d *DB) SetConfig(conf *Conf) error {
	d.Conf = conf

	for _, col := range d.Collections {
		col.Conf = conf
		for _, index := range col.Indexes {
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
	for i, col := range d.Collections {
		if err := col.DB.Close(); err != nil {
			errors = fmt.Sprintf("%s%s\n", errors, err.Error())
		}
		d.Collections[i] = nil
	}

	if d.ValueStore != nil {
		err := d.ValueStore.Close()
		if err != nil {
			errors = fmt.Sprintf("%s%s\n", errors, err.Error())
		}
	}

	if errors != "" {
		return fmt.Errorf(errors)
	}

	d.Path = ""
	d.ValueStore = nil
	d.Collections = nil

	d = nil
	return nil
}

// DeleteCollection delete the given collection
func (d *DB) DeleteCollection(collectionName string) error {
	var c *Collection
	for i, col := range d.Collections {
		if col.Name == collectionName {
			// Save the collection pointer for future cleanup
			c = col
			// Delete the collection form the list of collection pointers
			copy(d.Collections[i:], d.Collections[i+1:])
			d.Collections[len(d.Collections)-1] = nil
			d.Collections = d.Collections[:len(d.Collections)-1]
			break
		}
	}

	// Close index DB
	if err := c.DB.Close(); err != nil {
		return err
	}
	// Remove the index DB files
	if err := os.RemoveAll(d.Path + "/collections/" + c.ID); err != nil {
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

		err = d.ValueStore.Update(func(txn *badger.Txn) error {
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
