package db

import (
	"encoding/json"
	"fmt"
	"os"
)

func (c *Collection) save() error {
	return nil
}

func (c *Collection) load() error {
	if checkErr := c.checkDir(); checkErr != nil {
		return fmt.Errorf("directory is not usable: %s", checkErr.Error())
	}

	return nil
}

func (c *Collection) putObject(file *os.File, value interface{}) error {
	buf, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return fmt.Errorf("marshaling record: %s", marshalErr.Error())
	}

	return c.putToFile(file, buf)
}
func (c *Collection) putBin(file *os.File, value []byte) error {
	return c.putToFile(file, value)
}

func (c *Collection) putToFile(file *os.File, value []byte) error {
	n, writeErr := file.Write(value)
	if writeErr != nil {
		return fmt.Errorf("writing record: %s", writeErr.Error())
	}
	if n != len(value) {
		return fmt.Errorf("writing is not complet. has %d and writen %d", len(value), n)
	}

	return nil
}

func (c *Collection) openDoc(id string, bin bool, flags int) (*os.File, error) {
	return os.OpenFile(c.getRecordPath(id, bin), flags, filePermission)
}

func (c *Collection) getRecordPath(id string, bin bool) string {
	if bin {
		return fmt.Sprintf("%s/%s/%s/%s", c.path, recordsDirName, binsDirName, id)
	}
	return fmt.Sprintf("%s/%s/%s/%s", c.path, recordsDirName, objectsDirName, id)
}

func (c *Collection) checkDir() error {
	if _, err := os.Stat(c.path); os.IsNotExist(err) {
		return c.buildDir()
	}

	dirToCheck := []string{
		c.path + "/" + indexesDirName,
		c.path + "/" + recordsDirName,
		c.path + "/" + recordsDirName + "/" + binsDirName,
		c.path + "/" + recordsDirName + "/" + objectsDirName,
	}

	for _, dir := range dirToCheck {
		if !isDirOK(dir) {
			return fmt.Errorf("directory %q is not a good", dir)
		}
	}

	return nil
}

func (c *Collection) buildDir() error {
	if addDirErr := os.MkdirAll(c.path+"/"+indexesDirName, filePermission); addDirErr != nil {
		return fmt.Errorf("building the index directory: %s", addDirErr.Error())
	}

	if addDirErr := os.MkdirAll(c.path+"/"+recordsDirName+"/"+binsDirName, filePermission); addDirErr != nil {
		return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	}
	if addDirErr := os.MkdirAll(c.path+"/"+recordsDirName+"/"+objectsDirName, filePermission); addDirErr != nil {
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
