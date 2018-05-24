package collection

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gitea.interlab-net.com/alexandre/db/vars"
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

	fileNamePrefix := c.path + "/" + vars.RecordsDirName + "/" + vars.ObjectsDirName + "/"
	id := strings.Replace(file.Name(), fileNamePrefix, "", 1)
	c.updateIndex(id, value)

	return c.putToFile(file, buf)
}

func (c *Collection) putBin(file *os.File, value []byte) error {
	return c.putToFile(file, value)
}

func (c *Collection) putToFile(file *os.File, value []byte) error {
	n, writeErr := file.WriteAt(value, 0)
	if writeErr != nil {
		return fmt.Errorf("writing record: %s", writeErr.Error())
	}
	if n != len(value) {
		return fmt.Errorf("writing is not complet. has %d and writen %d", len(value), n)
	}

	return nil
}

func (c *Collection) openDoc(id string, bin bool, flags int) (*os.File, error) {
	return os.OpenFile(c.getRecordPath(id, bin), flags, vars.FilePermission)
}

func (c *Collection) getRecordPath(id string, bin bool) string {
	if bin {
		return fmt.Sprintf("%s/%s/%s/%s", c.path, vars.RecordsDirName, vars.BinsDirName, id)
	}
	return fmt.Sprintf("%s/%s/%s/%s", c.path, vars.RecordsDirName, vars.ObjectsDirName, id)
}

func (c *Collection) checkDir() error {
	if _, err := os.Stat(c.path); os.IsNotExist(err) {
		return c.buildDir()
	}

	dirToCheck := []string{
		c.path + "/" + vars.IndexesDirName,
		c.path + "/" + vars.RecordsDirName,
		c.path + "/" + vars.RecordsDirName + "/" + vars.BinsDirName,
		c.path + "/" + vars.RecordsDirName + "/" + vars.ObjectsDirName,
	}

	for _, dir := range dirToCheck {
		if !isDirOK(dir) {
			return fmt.Errorf("directory %q is not a good", dir)
		}
	}

	return nil
}

func (c *Collection) getFile(id string) (file *os.File, isBin bool, err error) {
	file, err = c.openDoc(id, false, vars.GetFlags)
	if err != nil {
		file, err = c.openDoc(id, true, vars.GetFlags)
		if err != nil {
			err = fmt.Errorf("opening record: %s", err.Error())
			return
		}
		isBin = true
	}

	return
}

func (c *Collection) buildDir() error {
	if addDirErr := os.MkdirAll(c.path+"/"+vars.IndexesDirName, vars.FilePermission); addDirErr != nil {
		return fmt.Errorf("building the index directory: %s", addDirErr.Error())
	}

	if addDirErr := os.MkdirAll(c.path+"/"+vars.RecordsDirName+"/"+vars.BinsDirName, vars.FilePermission); addDirErr != nil {
		return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	}
	if addDirErr := os.MkdirAll(c.path+"/"+vars.RecordsDirName+"/"+vars.ObjectsDirName, vars.FilePermission); addDirErr != nil {
		return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	}

	if addDirErr := os.MkdirAll(c.path+"/"+vars.MetaDatasDirName, vars.FilePermission); addDirErr != nil {
		return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	}

	file, indexFileErr := os.OpenFile(c.path+"/"+vars.MetaDatasDirName+"/indexes.json", vars.PutFlags, vars.FilePermission)
	if indexFileErr != nil {
		return fmt.Errorf("building the index mata data file: %s", indexFileErr.Error())
	}
	file.Close()

	return nil
}

func isDirOK(givenPath string) bool {
	dirFile, dirFileErr := os.OpenFile(givenPath, os.O_RDONLY, vars.FilePermission)
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
