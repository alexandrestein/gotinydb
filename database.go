package db

import (
	"fmt"
	"os"
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

	lockFile, addLockErr := os.OpenFile(d.path+"/"+lockFileName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePermission)
	if addLockErr != nil {
		return nil, fmt.Errorf("setting lock: %s", addLockErr.Error())
	}
	d.lockFile = lockFile

	return d, nil
}

// Use will try to get the collection from active ones. If not active it loads
// it from drive and if not existing it builds it.
func (d *DB) Use(colName string) (*Collection, error) {
	// Gets from in memory
	if activeCol, found := d.Collections[colName]; found {
		return activeCol, nil
	}

	// Gets from drive
	col := NewCollection(d.getPathFor(colName))
	if err := col.load(); err != nil {
		return nil, fmt.Errorf("loading collection: %s", err.Error())
	}

	// Save the collection in memory
	d.Collections[colName] = col

	return col, nil
}

// Close removes the lock file
func (d *DB) Close() {
	os.Remove(d.path + "/" + lockFileName)
}

// CloseCollection clean the collection slice of the object of the collection
func (d *DB) CloseCollection(colName string) {
	delete(d.Collections, colName)
}

func (d *DB) getPathFor(colName string) string {
	return fmt.Sprintf("%s/%s", d.path, colName)
}

func (d *DB) buildRootDir() error {
	if makeRootDirErr := os.MkdirAll(d.path, filePermission); makeRootDirErr != nil {
		if os.IsExist(makeRootDirErr) {
			return nil
		}
		return fmt.Errorf("building root directory: %s", makeRootDirErr.Error())
	}
	return nil
}
