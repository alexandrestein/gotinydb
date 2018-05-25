// Package collection is the "storage structur" of the database package.
// As most of the NO-SQL databases collections are the main part of the
// databases.
package collection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"gitea.interlab-net.com/alexandre/db/index"
	"gitea.interlab-net.com/alexandre/db/query"
	"gitea.interlab-net.com/alexandre/db/vars"
	"github.com/emirpasic/gods/utils"
)

// NewCollection builds a new Collection pointer. It is called internaly by DB
func NewCollection(path string) (*Collection, error) {
	c := new(Collection)
	c.path = path
	c.indexes = map[string]index.Index{}

	if err := c.load(); err != nil {
		return nil, fmt.Errorf("loading DB: %s", err.Error())
	}

	return c, nil
}

// Put saves the given element into the given ID.
// If record already exists this update it.
func (c *Collection) Put(id string, value interface{}) error {
	isBin := false
	binAsBytes := []byte{}
	if bytes, ok := value.([]byte); ok {
		isBin = true
		binAsBytes = bytes
	}

	file, openErr := c.openDoc(id, isBin, vars.PutFlags)
	if openErr != nil {
		return fmt.Errorf("opening record: %s", openErr.Error())
	}
	defer file.Close()

	if isBin {
		if err := c.putBin(file, binAsBytes); err != nil {
			return err
		}
		return nil
	}

	if err := c.putObject(file, value); err != nil {
		return err
	}
	return nil
}

// Get fillups the given value from the given ID. If you want to get binary
// content you must give a bytes.Buffer pointer. For structs or objects is use
// the encoding/json package to save and restor obects.
func (c *Collection) Get(id string, value interface{}) error {
	if id == "" {
		return fmt.Errorf("id can't be empty")
	}

	file, isBin, openErr := c.getFile(id)
	if openErr != nil {
		return openErr
	}
	defer file.Close()

	ret := []byte{}
	readOffSet := int64(0)
	for {
		buf := make([]byte, vars.BlockSize)
		n, readErr := file.ReadAt(buf, readOffSet)
		if readErr != nil {
			if readErr == io.EOF {
				buf = buf[:n]
				ret = append(ret, buf...)
				break
			}
			return fmt.Errorf("reading record: %s", readErr.Error())
		}
		readOffSet = readOffSet + int64(n)
		ret = append(ret, buf...)
	}

	if isBin {
		if givenBuffer, ok := value.(*bytes.Buffer); ok {
			givenBuffer.Write(ret)
			return nil
		}
		return fmt.Errorf("reciever is not a bytes.Buffer pointer")
	}
	if umarshalErr := json.Unmarshal(ret, value); umarshalErr != nil {
		return fmt.Errorf("umarshaling record: %s", umarshalErr.Error())
	}

	return nil
}

// Delete removes the coresponding object and index references.
func (c *Collection) Delete(id string) error {
	defer os.Remove(c.getRecordPath(id, false))
	defer os.Remove(c.getRecordPath(id, true))

	refs, getRefsErr := c.getIndexReference(id)
	if getRefsErr != nil {
		return fmt.Errorf("getting the index references: %s", getRefsErr.Error())
	}

	if err := c.updateIndexAfterDelete(id, refs); err != nil {
		return fmt.Errorf("updating index: %s", err.Error())
	}

	return c.deleteIndexRefFile(id)
}

// SetIndex adds new index to the collection
func (c *Collection) SetIndex(name string, indexType utils.ComparatorType, selector []string) error {
	if c.indexes[name] != nil {
		return fmt.Errorf("index %q already exists", name)
	}

	switch indexType {
	case utils.StringComparatorType:
		c.indexes[name] = index.NewString(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.IntComparatorType:
		c.indexes[name] = index.NewInt(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.Int8ComparatorType:
		c.indexes[name] = index.NewInt8(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.Int16ComparatorType:
		c.indexes[name] = index.NewInt16(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.Int32ComparatorType:
		c.indexes[name] = index.NewInt32(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.Int64ComparatorType:
		c.indexes[name] = index.NewInt64(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.UIntComparatorType:
		c.indexes[name] = index.NewUint(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.UInt8ComparatorType:
		c.indexes[name] = index.NewUint8(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.UInt16ComparatorType:
		c.indexes[name] = index.NewUint16(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.UInt32ComparatorType:
		c.indexes[name] = index.NewUint32(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.UInt64ComparatorType:
		c.indexes[name] = index.NewUint64(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.Float32ComparatorType:
		c.indexes[name] = index.NewFloat32(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.Float64ComparatorType:
		c.indexes[name] = index.NewFloat64(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case utils.TimeComparatorType:
		c.indexes[name] = index.NewTime(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	}

	return c.save()
}

// GetIndex return the coreponding index.
func (c *Collection) GetIndex(indexName string) index.Index {
	return c.indexes[indexName]
}

// Query run the given query to all the collection indexes.
// It returns the
func (c *Collection) Query(q *query.Query) (ids []string) {
	for _, index := range c.indexes {
		ids = append(ids, index.RunQuery(q)...)
	}

	return ids
}
