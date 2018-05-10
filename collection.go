package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// NewCollection builds a new Collection pointer. It is called internaly by DB
func NewCollection(path string) *Collection {
	c := new(Collection)
	c.path = path

	return c
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

	file, openErr := c.openDoc(id, isBin, putFlags)
	if openErr != nil {
		return fmt.Errorf("opening record: %s", openErr.Error())
	}

	if isBin {
		return c.putBin(file, binAsBytes)
	}

	return c.putObject(file, value)
}

// Get fillups the given value from the given ID. If you want to get binary
// content you must give a bytes.Buffer pointer.
func (c *Collection) Get(id string, value interface{}) error {
	isBin := false

	file, openErr := c.openDoc(id, false, getFlags)
	if openErr != nil {
		file, openErr = c.openDoc(id, true, getFlags)
		if openErr != nil {
			return fmt.Errorf("opening record: %s", openErr.Error())
		}
		isBin = true
	}

	ret := []byte{}
	readOffSet := int64(0)
	for {
		buf := make([]byte, blockSize)
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

// SetIndex adds new index to the collection
func (c *Collection) SetIndex(target string) error {
	return nil
}
