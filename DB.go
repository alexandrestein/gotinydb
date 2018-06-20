package gotinydb

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger"
)

type (
	// DB is the main element of the package and provide all access to sub commandes
	DB struct {
		Path        string
		ValueStore  *badger.DB
		Collections []*Collection

		ctx context.Context
	}
)

// Open simply opens a new or existing database
func Open(ctx context.Context, path string) (*DB, error) {
	d := new(DB)
	d.Path = path
	d.ctx = ctx
	if err := d.buildPath(); err != nil {
		return nil, err
	}

	if initBadgerErr := d.initBadger(); initBadgerErr != nil {
		return nil, initBadgerErr
	}

	if loadErr := d.loadCollections(); loadErr != nil {
		return nil, loadErr
	}

	return d, nil
}

// Use build or get a Collection pointer
func (d *DB) Use(colName string) (*Collection, error) {
	for _, col := range d.Collections {
		if col.Name == colName {
			return col, nil
		}
	}

	c, loadErr := d.getCollection("", colName)
	if loadErr != nil {
		return nil, loadErr
	}

	d.Collections = append(d.Collections, c)

	return c, nil
}

// Close close the underneath collections and main store
func (d *DB) Close() error {
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
