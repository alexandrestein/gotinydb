package GoTinyDB

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/alexandreStein/GoTinyDB/vars"
	bolt "github.com/coreos/bbolt"
)

type (
	// IndexReference is design to make easy clean up of the index after update
	// or delete.
	// The identifaction of the references are the coresponding ID.
	IndexReference struct {
		IndexName string
		Value     interface{}

		StringValue  string    `json:",omitempty"`
		IntValue     int       `json:",omitempty"`
		Int8aVlue    int8      `json:",omitempty"`
		Int16Value   int16     `json:",omitempty"`
		Int32Value   int32     `json:",omitempty"`
		Int64Value   int64     `json:",omitempty"`
		UintValue    uint      `json:",omitempty"`
		Uint8Value   uint8     `json:",omitempty"`
		Uint16Value  uint16    `json:",omitempty"`
		Uint32Value  uint32    `json:",omitempty"`
		Uint64Value  uint64    `json:",omitempty"`
		Float32Value float32   `json:",omitempty"`
		Float64Value float64   `json:",omitempty"`
		TimeValue    time.Time `json:",omitempty"`
	}
)

func newIndexReference(indexName string, value interface{}) *IndexReference {
	ref := &IndexReference{IndexName: indexName}
	if typedValue, ok := value.(string); ok {
		ref.StringValue = typedValue
	} else if typedValue, ok := value.(int); ok {
		ref.IntValue = typedValue
	} else if typedValue, ok := value.(int8); ok {
		ref.Int8aVlue = typedValue
	} else if typedValue, ok := value.(int16); ok {
		ref.Int16Value = typedValue
	} else if typedValue, ok := value.(int32); ok {
		ref.Int32Value = typedValue
	} else if typedValue, ok := value.(int64); ok {
		ref.Int64Value = typedValue
	} else if typedValue, ok := value.(uint); ok {
		ref.UintValue = typedValue
	} else if typedValue, ok := value.(uint8); ok {
		ref.Uint8Value = typedValue
	} else if typedValue, ok := value.(uint16); ok {
		ref.Uint16Value = typedValue
	} else if typedValue, ok := value.(uint32); ok {
		ref.Uint32Value = typedValue
	} else if typedValue, ok := value.(uint64); ok {
		ref.Uint64Value = typedValue
	} else if typedValue, ok := value.(float32); ok {
		ref.Float32Value = typedValue
	} else if typedValue, ok := value.(float64); ok {
		ref.Float64Value = typedValue
	} else if typedValue, ok := value.(time.Time); ok {
		ref.TimeValue = typedValue
	}
	return ref
}

// GetValue returns the correctly typed value inside an interface.
func (ref *IndexReference) GetValue() interface{} {
	if ref.StringValue != "" {
		return ref.StringValue
	} else if ref.IntValue != 0 {
		return ref.IntValue
	} else if ref.Int8aVlue != 0 {
		return ref.Int8aVlue
	} else if ref.Int16Value != 0 {
		return ref.Int16Value
	} else if ref.Int32Value != 0 {
		return ref.Int32Value
	} else if ref.Int64Value != 0 {
		return ref.Int64Value
	} else if ref.UintValue != 0 {
		return ref.UintValue
	} else if ref.Uint8Value != 0 {
		return ref.Uint8Value
	} else if ref.Uint16Value != 0 {
		return ref.Uint16Value
	} else if ref.Uint32Value != 0 {
		return ref.Uint32Value
	} else if ref.Uint64Value != 0 {
		return ref.Uint64Value
	} else if ref.Float32Value != 0 {
		return ref.Float32Value
	} else if ref.Float64Value != 0 {
		return ref.Float64Value
	} else if !ref.TimeValue.IsZero() {
		return ref.TimeValue
	}
	return nil
}

func (c *Collection) setIndexReferences(id string, refs []*IndexReference) error {
	// encoder := json.NewEncoder(file)
	// encodeErr := encoder.Encode(refs)
	// if encodeErr != nil {
	// 	return fmt.Errorf("encoding %q references: %s", id, encodeErr.Error())
	// }
	refsAsBytes, marshalErr := json.Marshal(refs)
	if marshalErr != nil {
		return fmt.Errorf("converting the references to JSON: %s", marshalErr.Error())
	}

	if err := c.boltDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(vars.InternalBuckectMetaDatas).Put([]byte(id), refsAsBytes)
	}); err != nil {
		return fmt.Errorf("saving references into DB: %s", err.Error())
	}

	return nil
}

// func (c *Collection) getIndexReferences(id string) ([]*IndexReference, error) {
// 	file, references, openErr := c.loadIndexRefAndFile(id, false)
// 	if openErr != nil {
// 		return nil, openErr
// 	}
// 	defer file.Close()
//
// 	return references, nil
// }

func (c *Collection) getIndexReferences(id string) ([]*IndexReference, error) {
	// file, openErr := c.getIndexRefFile(id, update)
	// if openErr != nil {
	// 	return nil, nil, openErr
	// }
	// if file == nil {
	// 	return nil, []*IndexReference{}, nil
	// }
	refsAsBytes := []byte{}

	if viewErr := c.boltDB.View(func(tx *bolt.Tx) error {
		refsAsBytes = tx.Bucket(vars.InternalBuckectMetaDatas).Bucket([]byte(c.Name)).Get([]byte(id))
		return nil
	}); viewErr != nil {
		return nil, fmt.Errorf("getting the references: %s", viewErr)
	}

	refs := []*IndexReference{}

	decodeErr := json.Unmarshal(refsAsBytes, &refs)
	if decodeErr != nil {
		return []*IndexReference{}, nil
	}

	return refs, nil
}

// func (c *Collection) getIndexRefFile(id string, update bool) (ret *os.File, err error) {
// 	log.Print("getIndexRef")
// 	return nil, nil
//
// 	// openFlags := vars.GetFlags
// 	// if update {
// 	// openFlags = vars.PutFlags
// 	// }
// 	//
// 	// indexMetaFile, openErr := os.OpenFile(c.path+"/"+vars.MetaDatasDirName+"/"+id+".json", openFlags, vars.FilePermission)
// 	// if openErr != nil {
// 	// if !update && fmt.Sprintf("%T", openErr) == "*os.PathError" {
// 	// return nil, nil
// 	// }
// 	// return nil, fmt.Errorf("openning index reference data file %q: %s", id, openErr.Error())
// 	// }
// 	//
// 	// return indexMetaFile, nil
// }

func (c *Collection) deleteIndexRefFile(id string) (err error) {
	if err := c.boltDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(vars.InternalBuckectMetaDatas).Bucket([]byte(c.Name)).Delete([]byte(id))
	}); err != nil {
		return err
	}
	return nil
}
