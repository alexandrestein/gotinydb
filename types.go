package db

import (
	"os"

	"gitea.interlab-net.com/alexandre/db/collection"
)

type (
	DB struct {
		Collections map[string]*collection.Collection
		path        string
		lockFile    *os.File
	}

	Record struct {
		Type RecordType
		Size int
		Data interface{}
	}

	IndexType  int
	RecordType int
)
