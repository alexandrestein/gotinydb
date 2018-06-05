package gotinydb

import (
	"crypto/sha1"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

var (
	getTestPathChan chan string
)

func init() {
	getTestPathChan = make(chan string)
	buf, _ := time.Now().MarshalBinary()
	randBytes := sha1.Sum(buf)
	randPart := fmt.Sprintf("%x", randBytes[:4])
	nTest := 0
	go func() {
		for {
			path := fmt.Sprintf("%s/gotinydb-%s-%d", os.TempDir(), randPart, nTest)
			getTestPathChan <- path
			nTest++
		}
	}()
}

func TestOpenAndClose(t *testing.T) {
	testPath := <-getTestPathChan
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
	testPath := <-getTestPathChan
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

	if err := c.Put("testID", nil); err != nil {
		t.Error(err)
		return
	}
}

func TestLoadCollection(t *testing.T) {
	testPath := <-getTestPathChan
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

	testUser := struct {
		Login, Pass string
	}{"User 1", "super password"}
	if err := c.Put("testID", &testUser); err != nil {
		t.Error(err)
		return
	}

	if err := db.Close(); err != nil {
		t.Error(err)
		return
	}

	db2, openDBerr2 := Open(testPath)
	if openDBerr2 != nil {
		t.Error(openDBerr2)
		return
	}
	defer db.Close()

	c2, userErr2 := db2.Use("testCol")
	if userErr2 != nil {
		t.Error(userErr2)
		return
	}

	retrieveTestUser := struct {
		Login, Pass string
	}{}
	if getErr := c2.Get("testID", &retrieveTestUser); getErr != nil {
		t.Error(getErr)
		return
	}

	if !reflect.DeepEqual(testUser, retrieveTestUser) {
		t.Error("given object and retreive on are not equal")
		return
	}
}
