package gotinydb

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/alexandrestein/gotinydb/vars"
	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

func (d *DB) buildPath() error {
	return os.MkdirAll(d.Path+"/collections", vars.FilePermission)
}

func (d *DB) initBadger() error {
	opts := badger.DefaultOptions
	opts.Dir = d.Path + "/store"
	opts.ValueDir = d.Path + "/store"
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}

	d.ValueStore = db
	return nil
}

func (d *DB) loadCollections() error {
	colsIDs, getColsIDsErr := d.getCollectionsIDs()
	if getColsIDsErr != nil {
		return getColsIDsErr
	}
	for _, colID := range colsIDs {
		col, err := d.getCollection(colID, "")
		if err != nil {
			return err
		}

		d.Collections = append(d.Collections, col)
	}

	return nil
}

func (d *DB) getCollection(colID, colName string) (*Collection, error) {
	c := new(Collection)
	c.Store = d.ValueStore

	if colID == "" && colName == "" {
		return nil, fmt.Errorf("name and ID can't be empty")
	} else if colID == "" {
		colID = vars.BuildID(colName)
	}

	c.ID = colID
	c.Name = colName

	db, openDBerr := bolt.Open(d.Path+"/collections/"+colID, vars.FilePermission, nil)
	if openDBerr != nil {
		return nil, openDBerr
	}
	c.DB = db

	// Try to load the collection informations
	if err := c.loadInfos(); err != nil {
		// If not exists try to build it
		if err == vars.ErrNotFound {
			if colName == "" {
				return nil, fmt.Errorf("init collection but have empty name")
			}
			err = c.init(colName)
			// Error after at build
			if err != nil {
				return nil, err
			}
			// No error return the new Collection pointer
			return c, nil
		}
		// Other error than not found
		return nil, err
	}

	// The collection is loaded and database is ready
	return c, nil
}

func (d *DB) getCollectionsIDs() ([]string, error) {
	files, err := ioutil.ReadDir(d.Path + "/collections")
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, f := range files {
		ret = append(ret, f.Name())
	}
	return ret, nil
}
