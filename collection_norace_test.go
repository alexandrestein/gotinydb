// +build !race

package gotinydb

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestCollection_PutToCloseDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "putToBadDB"
	defer os.RemoveAll(testPath)

	db, err := Open(ctx, NewDefaultOptions(testPath))
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	c, err := db.Use("user collection")
	if err != nil {
		t.Error(err)
		return
	}

	cancel()

	obj := &struct{ Name string }{"Bad Insertion"}
	err = c.Put("hello", obj)
	if err != ErrClosedDB {
		t.Error("The database must return an error but not the one expected", err)
		return
	}
}

// func TestDB_SetOptions(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "setOptions"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	defer db.Close()

// 	_, err = db.Use("testCol")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	db.SetOptions(NewDefaultOptions(testPath))
// }
