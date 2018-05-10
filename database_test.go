package db

import (
	"fmt"
	"os"
	"testing"
)

func TestDB(t *testing.T) {
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	db, initErr := New(path)
	if initErr != nil {
		t.Error(initErr.Error())
		return
	}

	userCol, userColErr := db.Use("col1")
	if userColErr != nil {
		t.Errorf("openning test collection: %s", userColErr.Error())
		return
	}

	fmt.Println(userCol)
}
