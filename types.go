package GoTinyDB

import (
<<<<<<< HEAD
	"github.com/alexandreStein/GoTinyDB/collection"
	bolt "github.com/coreos/bbolt"
=======
	"os"

	"github.com/alexandreStein/GoTinyDB/collection"
>>>>>>> indexes
)

type (
	DB struct {
		collections map[string]*collection.Collection
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
