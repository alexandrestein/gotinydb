// Package collection is the "storage structur" of the database package.
// As most of the NO-SQL databases collections are the main part of the
// databases.
package collection

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alexandreStein/GoTinyDB/index"
	"github.com/alexandreStein/GoTinyDB/query"
	"github.com/alexandreStein/GoTinyDB/vars"
	"github.com/alexandreStein/gods/utils"
	bolt "github.com/coreos/bbolt"

	"github.com/labstack/gommon/log"
)

// NewCollection builds a new Collection pointer. It is called internaly by DB
func NewCollection(db *bolt.DB, name string) *Collection {
	c := new(Collection)
	c.Name = name
	c.boltDB = db

	c.Indexes = map[string]index.Index{}

	// if err := c.load(); err != nil {
	// 	return nil, fmt.Errorf("loading DB: %s", err.Error())
	// }

	return c
}

// func (c *Collection) SetBolt(db *bolt.DB) {
// 	c.boltDB = db
// }

// Put saves the given element into the given ID.
// If record already exists it updates it.
// If the goal is to store stream of bytes you need to send []byte{} inside
// the interface.
func (c *Collection) Put(id string, value interface{}) error {
	isBin := false
	valueAsBytes := []byte{}
	if bytes, ok := value.([]byte); ok {
		isBin = true
		valueAsBytes = bytes
	}

	if !isBin {
		jsonBytes, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return marshalErr
		}

		valueAsBytes = jsonBytes
	}

	if insertErr := c.boltDB.Update(func(tx *bolt.Tx) error {
		colBucket := tx.Bucket(vars.InternalBuckectCollections).Bucket([]byte(c.Name))
		if colBucket == nil {
			colBucket, _ = tx.Bucket(vars.InternalBuckectCollections).CreateBucket([]byte(c.Name))
		}
		return colBucket.Put([]byte(id), valueAsBytes)
	}); insertErr != nil {
		return insertErr
	}

	indexErrors := map[string]error{}
	for indexName, index := range c.Indexes {
		if val, apply := index.Apply(value); apply {
			if updateErr := c.updateIndex(id, val); updateErr != nil {
				indexErrors[indexName] = updateErr
			}
		}
	}

	if len(indexErrors) > 1 {
		errorString := "updating the index: "
		for indexName, err := range indexErrors {
			errorString += fmt.Sprintf("index %q: %s\n", indexName, err.Error())
		}
		return fmt.Errorf(errorString)
	}
	return nil

	// file, openErr := c.openDoc(id, isBin, vars.PutFlags)
	// if openErr != nil {
	// 	return fmt.Errorf("opening record: %s", openErr.Error())
	// }
	// defer file.Close()
	//
	// if isBin {
	// 	if err := c.putBin(file, binAsBytes); err != nil {
	// 		return err
	// 	}
	// 	return nil
	// }
	//
	// if err := c.putObject(file, value); err != nil {
	// 	return err
	// }
	// return nil
}

// Get fillups the given value from the given ID. If you want to get binary
// content you must give a bytes.Buffer pointer. For structs or objects is use
// the encoding/json package to save and restor obects.
func (c *Collection) Get(id string, value interface{}) error {
	if id == "" {
		return fmt.Errorf("id can't be empty")
	}

	contentAsBytes := []byte{}

	err := c.boltDB.View(func(tx *bolt.Tx) error {
		colBucket := tx.Bucket(vars.InternalBuckectCollections).Bucket([]byte(c.Name))
		if colBucket == nil {
			return fmt.Errorf("bucket of the collection %q does not exist", c.Name)
		}

		contentAsBytes = colBucket.Get([]byte(id))
		return nil
	})
	if err != nil {
		return err
	}

	if givenBuffer, ok := value.(*bytes.Buffer); ok {
		givenBuffer.Write(contentAsBytes)
		return nil
	}

	uMarshalErr := json.Unmarshal(contentAsBytes, value)
	if uMarshalErr != nil {
		return uMarshalErr
	}

	return nil

	// file, isBin, openErr := c.getFile(id)
	// if openErr != nil {
	// 	return openErr
	// }
	// defer file.Close()
	//
	// ret := []byte{}
	// readOffSet := int64(0)
	// for {
	// 	buf := make([]byte, vars.BlockSize)
	// 	n, readErr := file.ReadAt(buf, readOffSet)
	// 	if readErr != nil {
	// 		if readErr == io.EOF {
	// 			buf = buf[:n]
	// 			ret = append(ret, buf...)
	// 			break
	// 		}
	// 		return fmt.Errorf("reading record: %s", readErr.Error())
	// 	}
	// 	readOffSet = readOffSet + int64(n)
	// 	ret = append(ret, buf...)
	// }
	//
	// if isBin {
	// 	if givenBuffer, ok := value.(*bytes.Buffer); ok {
	// 		givenBuffer.Write(ret)
	// 		return nil
	// 	}
	// 	return fmt.Errorf("reciever is not a bytes.Buffer pointer")
	// }
	// if umarshalErr := json.Unmarshal(ret, value); umarshalErr != nil {
	// 	return fmt.Errorf("umarshaling record: %s", umarshalErr.Error())
	// }
	//
	// return nil
}

// Delete removes the coresponding object and index references.
func (c *Collection) Delete(id string) error {
	// defer os.Remove(c.getRecordPath(id, false))
	// defer os.Remove(c.getRecordPath(id, true))

	log.Print("DELETE")
	return nil

	refs, getRefsErr := c.getIndexReferences(id)
	if getRefsErr != nil {
		return fmt.Errorf("getting the index references: %s", getRefsErr.Error())
	}

	if err := c.updateIndexAfterDelete(id, refs); err != nil {
		return fmt.Errorf("updating index: %s", err.Error())
	}

	return nil
	// return c.deleteIndexRefFile(id)
}

// SetIndex adds new index to the collection
func (c *Collection) SetIndex(name string, indexType utils.ComparatorType, selector []string) error {
	if c.Indexes[name] != nil {
		return fmt.Errorf("index %q already exists", name)
	}

	switch indexType {
	case utils.StringComparatorType:
		c.Indexes[name] = index.NewString(name, selector)
	case utils.IntComparatorType:
		c.Indexes[name] = index.NewInt(name, selector)
	case utils.Int8ComparatorType:
		c.Indexes[name] = index.NewInt8(name, selector)
	case utils.Int16ComparatorType:
		c.Indexes[name] = index.NewInt16(name, selector)
	case utils.Int32ComparatorType:
		c.Indexes[name] = index.NewInt32(name, selector)
	case utils.Int64ComparatorType:
		c.Indexes[name] = index.NewInt64(name, selector)
	case utils.UIntComparatorType:
		c.Indexes[name] = index.NewUint(name, selector)
	case utils.UInt8ComparatorType:
		c.Indexes[name] = index.NewUint8(name, selector)
	case utils.UInt16ComparatorType:
		c.Indexes[name] = index.NewUint16(name, selector)
	case utils.UInt32ComparatorType:
		c.Indexes[name] = index.NewUint32(name, selector)
	case utils.UInt64ComparatorType:
		c.Indexes[name] = index.NewUint64(name, selector)
	case utils.Float32ComparatorType:
		c.Indexes[name] = index.NewFloat32(name, selector)
	case utils.Float64ComparatorType:
		c.Indexes[name] = index.NewFloat64(name, selector)
	case utils.TimeComparatorType:
		c.Indexes[name] = index.NewTime(name, selector)
	}

	return c.save()
}

// GetIndex return the coreponding index.
func (c *Collection) GetIndex(indexName string) index.Index {
	return c.Indexes[indexName]
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
				fmt.Println("11111", retIDs)
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
