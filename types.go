package GoTinyDB

import (
	bolt "github.com/coreos/bbolt"
)

type (
	DB struct {
		collections map[string]*Collection
		boltDB      *bolt.DB
		path        string
		// lockFile    *os.File
	}

	Record struct {
		Type RecordType
		Size int
		Data interface{}
	}

	IndexType  int
	RecordType int
)
