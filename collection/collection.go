package collection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/alexandreStein/GoTinyDB/index"
	"github.com/alexandreStein/GoTinyDB/query"
	"github.com/alexandreStein/GoTinyDB/vars"
)

// NewCollection builds a new Collection pointer. It is called internaly by DB
func NewCollection(path string) (*Collection, error) {
	c := new(Collection)
	c.path = path
	c.Indexes = map[string]index.Index{}

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
// content you must give a bytes.Buffer pointer.
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
func (c *Collection) SetIndex(name string, indexType index.Type, selector []string) error {
	if c.Indexes[name] != nil {
		return fmt.Errorf("index %q already exists", name)
	}

	switch indexType {
	case index.StringIndexType:
		c.Indexes[name] = index.NewStringIndex(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	case index.IntIndexType:
		c.Indexes[name] = index.NewIntIndex(c.path+"/"+vars.IndexesDirName+"/"+name, selector)
	}

	return c.save()
}

// Query run the given query to all the collection indexes.
func (c *Collection) Query(q *query.Query) (ids []string) {
	if q == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*500)
	defer cancel()

	getIDsChan := make(chan []string, 16)
	getIDs := []string{}
	keepIDsChan := make(chan []string, 16)
	keepIDs := []string{}

	for _, index := range c.Indexes {
		go index.RunQuery(ctx, q.GetActions, getIDsChan)
		go index.RunQuery(ctx, q.KeepActions, keepIDsChan)
	}

	getDone, keepDone := false, false

	for {
		select {
		case retIDs, ok := <-getIDsChan:
			if ok {
				getIDs = append(getIDs, retIDs...)
			} else {
				getDone = true
			}

			if getDone && keepDone {
				goto afterFilters
			}
		case retIDs, ok := <-keepIDsChan:
			if ok {
				keepIDs = append(keepIDs, retIDs...)
			} else {
				keepDone = true
			}

			if getDone && keepDone {
				goto afterFilters
			}
		case <-ctx.Done():
			return
		}
	}

afterFilters:
	ids = getIDs

	// Clean the retreived IDs of the keep selection
	for j := len(ids) - 1; j >= 0; j-- {
		for _, keepID := range keepIDs {
			if len(ids) <= j {
				continue
			}
			if ids[j] == keepID {
				ids = append(ids[:j], ids[j+1:]...)
				continue
			}
		}
		if q.Distinct {
			keys := make(map[string]bool)
			list := []string{}
			if _, value := keys[ids[j]]; !value {
				keys[ids[j]] = true
				list = append(list, ids[j])
			}
			ids = list
		}
	}

	// Do the limit
	if len(ids) > q.Limit {
		ids = ids[:q.Limit]
	}

	// Reverts the result if wanted
	if q.InvertedOrder {
		for i := len(ids)/2 - 1; i >= 0; i-- {
			opp := len(ids) - 1 - i
			ids[i], ids[opp] = ids[opp], ids[i]
		}
	}

	return ids
}
