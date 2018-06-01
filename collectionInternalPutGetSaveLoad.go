package GoTinyDB

import (
	"os"

	"github.com/alexandreStein/GoTinyDB/vars"
)

func (c *Collection) save() error {
	return nil
}

// func (c *Collection) load() error {
// 	boltDB, openBoltErr := bolt.Open(c.path+vars.BoltFileName, vars.FilePermission, nil)
// 	if openBoltErr != nil {
// 		return fmt.Errorf("openning bolt DB %q: %s", c.path, openBoltErr.Error())
// 	}
// 	c.boltDB = boltDB
//
// 	return c.loadIndexes()
// }

func (c *Collection) loadIndexes() error {
	// if err := c.boltDB.View(func(tx *bolt.Tx) error {
	// 	// Gets the indexes names from DB.
	// 	// v is a list of strings as bytes slice.
	// 	v := tx.Bucket(vars.InternalBuckectMetaDatas).Bucket([]byte(c.name)).Get([]byte(vars.IndexeNamesReservedName))
	//
	// 	indexNames := []string{}
	// 	unmarshalErr := json.Unmarshal(v, &indexNames)
	// 	if unmarshalErr != nil {
	// 		return fmt.Errorf("index names: %s", unmarshalErr.Error())
	// 	}
	//
	// 	return nil
	// }); err != nil {
	// 	return fmt.Errorf("loading indexes: %s", err.Error())
	// }
	return nil
}

func (c *Collection) loadIndex(name string) {

}

// func (c *Collection) openDoc(id string, bin bool, flags int) (*os.File, error) {
// 	return os.OpenFile(c.getRecordPath(id, bin), flags, vars.FilePermission)
// }

// func (c *Collection) checkDir() error {
// 	if _, err := os.Stat(c.path); os.IsNotExist(err) {
// 		return c.buildDir()
// 	}
//
// 	dirsToCheck := []string{
// 		c.path + "/" + vars.IndexesDirName,
// 	}
//
// 	for _, dir := range dirsToCheck {
// 		if !isDirOK(dir) {
// 			return fmt.Errorf("directory %q is not a good", dir)
// 		}
// 	}
//
// 	return nil
// }

// func (c *Collection) getFile(id string) (file *os.File, isBin bool, err error) {
// 	file, err = c.openDoc(id, false, vars.GetFlags)
// 	if err != nil {
// 		file, err = c.openDoc(id, true, vars.GetFlags)
// 		if err != nil {
// 			err = fmt.Errorf("opening record: %s", err.Error())
// 			return
// 		}
// 		isBin = true
// 	}
//
// 	return
// }

func (c *Collection) buildDir() error {
	// if addDirErr := os.MkdirAll(c.path+"/"+vars.IndexesDirName, vars.FilePermission); addDirErr != nil {
	// 	return fmt.Errorf("building the index directory: %s", addDirErr.Error())
	// }
	//
	// if addDirErr := os.MkdirAll(c.path+"/"+vars.RecordsDirName+"/"+vars.BinsDirName, vars.FilePermission); addDirErr != nil {
	// 	return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	// }
	// if addDirErr := os.MkdirAll(c.path+"/"+vars.RecordsDirName+"/"+vars.ObjectsDirName, vars.FilePermission); addDirErr != nil {
	// 	return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	// }
	//
	// if addDirErr := os.MkdirAll(c.path+"/"+vars.MetaDatasDirName, vars.FilePermission); addDirErr != nil {
	// 	return fmt.Errorf("building the record directory: %s", addDirErr.Error())
	// }
	//
	// file, indexFileErr := os.OpenFile(c.path+"/"+vars.MetaDatasDirName+"/indexes.json", vars.PutFlags, vars.FilePermission)
	// if indexFileErr != nil {
	// 	return fmt.Errorf("building the index mata data file: %s", indexFileErr.Error())
	// }
	// file.Close()

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
