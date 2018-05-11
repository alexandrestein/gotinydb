package db

import (
	"fmt"
	"os"

	"gitea.interlab-net.com/alexandre/db/collection"
	"gitea.interlab-net.com/alexandre/db/vars"
)

// New builds a new DB object with the given root path. It must be a directory.
// This path will be used to hold every elements. The entire data structur will
// be located in the directory.
func New(path string) (*DB, error) {
	d := new(DB)
	d.Collections = map[string]*Collection{}
	d.path = path
	if err := d.buildRootDir(); err != nil {
		return nil, fmt.Errorf("initializing DB: %s", err.Error())
	}

	lockFile, addLockErr := os.OpenFile(d.path+"/"+vars.LockFileName, vars.OpenDBFlags, vars.FilePermission)
	if addLockErr != nil {
		return nil, fmt.Errorf("setting lock: %s", addLockErr.Error())
	}
	d.lockFile = lockFile

	return d, nil
}

// Use will try to get the collection from active ones. If not active it loads
// it from drive and if not existing it builds it.
func (d *DB) Use(colName string) (*collection.Collection, error) {
	// Gets from in memory
	if activeCol, found := d.Collections[colName]; found {
		return activeCol, nil
	}

	// Gets from drive
	col, openColErr := collection.NewCollection(d.getPathFor(colName))
	if openColErr != nil {
		return nil, fmt.Errorf("loading collection: %s", openColErr.Error())
	}

	// Save the collection in memory
	d.Collections[colName] = col

	return col, nil
}

// Close removes the lock file
func (d *DB) Close() {
	os.Remove(d.path + "/" + vars.LockFileName)
}

// CloseCollection clean the collection slice of the object of the collection
func (d *DB) CloseCollection(colName string) {
	delete(d.Collections, colName)
}

func (d *DB) getPathFor(colName string) string {
	return fmt.Sprintf("%s/%s", d.path, colName)
}

func (d *DB) buildRootDir() error {
	if makeRootDirErr := os.MkdirAll(d.path, vars.FilePermission); makeRootDirErr != nil {
		if os.IsExist(makeRootDirErr) {
			return nil
		}
		return fmt.Errorf("building root directory: %s", makeRootDirErr.Error())
	}
	return nil
}
