package simple

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve"
)

var (
	db  *DB
	col *Collection

	path      = "testDB"
	configKey = [32]byte{}

	colName = "collection name"

	testUserID = "test ID"
	testUser   = &user{"toto", "toto@internet.org"}
)

type (
	user struct {
		Name, Email string
	}
)

func TestMain(t *testing.T) {
	defer clean()
	open(t)

}

func open(t *testing.T) {
	var err error
	db, err = New(path, configKey)
	if err != nil {
		t.Error(err)
		return
	}

	col, err = db.Use(colName)
	if err != nil {
		t.Error(err)
		return
	}

	err = col.SetBleveIndex("test", bleve.NewIndexMapping(), "Email")
	if err != nil {
		t.Error(err)
		return
	}

	err = col.Put(testUserID, testUser)
	if err != nil {
		t.Error(err)
		return
	}

}

func clean() {
	db.Close()
	os.RemoveAll(path)
}
