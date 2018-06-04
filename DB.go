package gotinydb

import (
	"fmt"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/dgraph-io/badger"
)

type (
	DB struct {
		Path        string
		ValueStore  *badger.DB
		Collections []*Collection
	}
)

func Open(path string) (*DB, error) {
	d := new(DB)
	d.Path = path
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

func (d *DB) Use(colName string) (*Collection, error) {
	for _, col := range d.Collections {
		if col.Name == colName {
			return col, nil
		}
	}

	c, loadErr := d.loadCollection(vars.BuildIDAsString(colName))
	if loadErr != nil {
		return nil, loadErr
	}

	d.Collections = append(d.Collections, c)

	return c, nil
}

func (d *DB) Close() error {

	errors := ""
	for _, col := range d.Collections {
		if err := col.DB.Close(); err != nil {
			errors = fmt.Sprintf("%s%s\n", errors, err.Error())
		}
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
