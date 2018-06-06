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
	if colID == "" {
		colID = vars.BuildIDAsString(colName)
	}

	c := new(Collection)
	c.Store = d.ValueStore
	c.ID = colID
	c.Name = colName
	fmt.Println("name and ID", colName, colID)
	fmt.Println("path", d.Path+"/collections/"+colID)

	db, openDBerr := bolt.Open(d.Path+"/collections/"+colID, vars.FilePermission, nil)
	if openDBerr != nil {
		return nil, openDBerr
	}
	c.DB = db

	fmt.Println("HOLDED")

	// Try to load the collection informations
	if err := c.loadInfos(); err != nil {
		// If not exists try to build it
		if err == vars.ErrNotFound {
			err = c.init(colID)
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
