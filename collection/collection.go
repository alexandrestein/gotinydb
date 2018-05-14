package collection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"gitea.interlab-net.com/alexandre/db/index"
	"gitea.interlab-net.com/alexandre/db/vars"
)

// NewCollection builds a new Collection pointer. It is called internaly by DB
func NewCollection(path string) (*Collection, error) {
	c := new(Collection)
	c.path = path

	if err := c.load(); err != nil {
		return nil, fmt.Errorf("loading DB: %s", err.Error())
	}

	return c, nil
}

// Put saves the given element into the given ID.
// If record already exists this update it.
func (c *Collection) Put(id string, value interface{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer c.updateIndex(ctx, value, id)

	isBin := false
	binAsBytes := []byte{}
	if bytes, ok := value.([]byte); ok {
		isBin = true
		binAsBytes = bytes
	}

	file, openErr := c.openDoc(id, isBin, vars.PutFlags)
	if openErr != nil {
		cancel()
		return fmt.Errorf("opening record: %s", openErr.Error())
	}

	if isBin {
		if err := c.putBin(file, binAsBytes); err != nil {
			cancel()
			return err
		}
	}

	if err := c.putObject(file, value); err != nil {
		cancel()
		return err
	}
	return nil
}

// Get fillups the given value from the given ID. If you want to get binary
// content you must give a bytes.Buffer pointer.
func (c *Collection) Get(id string, value interface{}) error {
	file, isBin, openErr := c.getFile(id)
	if openErr != nil {
		return openErr
	}

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

func (c *Collection) Delete(id string) error {
	if rmObjErr := os.Remove(c.getRecordPath(id, false)); rmObjErr != nil {
		if rmBinErr := os.Remove(c.getRecordPath(id, true)); rmBinErr != nil {
			return fmt.Errorf("the object do not exist")
		}
	}

	if err := c.updateIndexAfterDelete(id); err != nil {
		return fmt.Errorf("updating index: %s", err.Error())
	}

	return nil
}

// SetIndex adds new index to the collection
func (c *Collection) SetIndex(target string) error {
	return nil
}

func (c *Collection) Query(q *index.Query) {

}
