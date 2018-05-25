package GoTinyDB

import (
	"os"

	"github.com/alexandreStein/GoTinyDB/collection"
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
