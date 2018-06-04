package gotinydb

import (
	"fmt"
	"os"
	"testing"
)

var testPath = os.TempDir() + "/gotinydb"

func TestOpenAndClose(t *testing.T) {
	defer os.RemoveAll(testPath)
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}

	if err := db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestCreateCollection(t *testing.T) {
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()

	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	c.Put
}

func TestLoadCollection(t *testing.T) {
	defer os.RemoveAll(testPath)
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()

	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	fmt.Println(c)
}
