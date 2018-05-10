package db

import (
	"fmt"
	"os"
)

func NewCollection(path string) *Collection {
	c := new(Collection)
	c.path = path

	return c
}

func (c *Collection) save() error {
	return nil
}

func (c *Collection) load() error {
	if checkErr := c.checkDir(); checkErr != nil {
		return fmt.Errorf("directory is not usable: %s", checkErr.Error())
	}

	return nil
}

func (c *Collection) Put() error {
	return nil
}

func (c *Collection) Get() error {
	return nil
}

func (c *Collection) SetIndex(target string) error {
	return nil
}

func (c *Collection) checkDir() error {
	if _, err := os.Stat(c.path); os.IsNotExist(err) {
		return c.buildDir()
	}

	dirToCheck := []string{
		c.path + "/indexes",
		c.path + "/records/bin",
		c.path + "/records/json",
	}

	for _, dir := range dirToCheck {
		if !isDirOK(dir) {
			return fmt.Errorf("directory %q is not a good", dir)
		}
	}

	return nil
}

func (c *Collection) buildDir() error {
	if addDirErr := os.MkdirAll(c.path+"/indexes", filePermission); addDirErr != nil {
		return fmt.Errorf("building the index directory: %s", addDirErr.Error())
	}

	if addDirErr := os.MkdirAll(c.path+"/records/bin", filePermission); addDirErr != nil {
		return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	}
	if addDirErr := os.MkdirAll(c.path+"/records/json", filePermission); addDirErr != nil {
		return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	}

	return nil
}

func isDirOK(givenPath string) bool {
	dirFile, dirFileErr := os.OpenFile(givenPath, os.O_RDONLY, filePermission)
	if dirFileErr != nil {
		if os.IsNotExist(dirFileErr) {
		}
		return false
	}

	rootStats, rootStatsErr := dirFile.Stat()
	if rootStatsErr != nil {
		return false
	}

	if !rootStats.IsDir() {
		return false
	}

	return true
}
