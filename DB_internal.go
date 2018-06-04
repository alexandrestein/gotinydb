package gotinydb

import (
	"io/ioutil"
	"os"

	"github.com/alexandrestein/gotinydb/vars"
	bolt "github.com/coreos/bbolt"
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
	colsNames, getColsNameErr := d.getCollectionsNames()
	if getColsNameErr != nil {
		return getColsNameErr
	}
	for _, colName := range colsNames {
		col, err := d.loadCollection(colName)
		if err != nil {
			return err
		}

		d.Collections = append(d.Collections, col)
	}

	return nil
}

func (d *DB) loadCollection(colName string) (*Collection, error) {
	c := new(Collection)
	c.Store = d.ValueStore
	db, openDBerr := bolt.Open(d.Path+"/collections/"+colName, vars.FilePermission, nil)
	// db, openDBerr := bolt.Open(d.Path+"/collections/"+colName, vars.FilePermission, &bolt.Options{Timeout: time.Second * 1})
	if openDBerr != nil {
		return nil, openDBerr
	}
	c.DB = db

	return c, nil
}

func (d *DB) getCollectionsNames() ([]string, error) {
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
