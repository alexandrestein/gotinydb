package gotinydb

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
)

func (d *DB) buildPath() error {
	return os.MkdirAll(d.path+"/collections", FilePermission)
}

func (d *DB) initBadger() error {
	opts := badger.DefaultOptions
	opts.Dir = d.path + "/store"
	opts.ValueDir = d.path + "/store"
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}

	d.valueStore = db
	return nil
}

func (d *DB) waitForClose() {
	<-d.ctx.Done()
	d.Close()
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

		if err := col.loadIndex(); err != nil {
			return err
		}

		d.collections = append(d.collections, col)
	}

	return nil
}

func (d *DB) getCollection(colID, colName string) (*Collection, error) {
	c := new(Collection)
	c.store = d.valueStore
	c.id = colID
	c.name = colName

	c.conf = d.conf

	c.initWriteTransactionChan(d.ctx)

	if colID == "" && colName == "" {
		return nil, fmt.Errorf("name and ID can't be empty")
	} else if colID == "" {
		colID = buildID(colName)
	}

	c.id = colID
	c.name = colName
	c.ctx = d.ctx

	db, openDBErr := bolt.Open(d.path+"/collections/"+colID, FilePermission, nil)
	if openDBErr != nil {
		return nil, openDBErr
	}
	c.db = db

	// Try to load the collection information
	if err := c.loadInfos(); err != nil {
		// If not exists try to build it
		if err == ErrNotFound {
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
	files, err := ioutil.ReadDir(d.path + "/collections")
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, f := range files {
		ret = append(ret, f.Name())
	}
	return ret, nil
}
