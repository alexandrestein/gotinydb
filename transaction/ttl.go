package transaction

import (
	"encoding/json"
	"time"
)

type (
	// TTL defines a type to permit to delete documents or files after a certain duration
	TTL struct {
		CleanTime              time.Time
		File                   bool
		DocumentCollectionName string
		DocumentID             string
	}
)

func (t *TTL) TimeAsKey(prefix []byte) []byte {
	ret, _ := t.CleanTime.MarshalBinary()
	return append(prefix, ret...)
}
func (t *TTL) ExportAsBytes() []byte {
	ret, _ := json.Marshal(t)
	return ret
}

func ParseTTL(input []byte) (*TTL, error) {
	obj := new(TTL)
	err := json.Unmarshal(input, obj)
	return obj, err
}

func NewTTL(colID, docOrFileID string, file bool, ttl time.Duration) *TTL {
	if ttl <= 0 {
		return nil
	}

	// var cleanTime time.Time
	// if ttl > 0 {
	// 	cleanTime = time.Now().Add(ttl)
	// }

	return &TTL{
		CleanTime:              time.Now().Add(ttl),
		File:                   file,
		DocumentCollectionName: colID,
		DocumentID:             docOrFileID,
	}
}
