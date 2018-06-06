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

	// Test that the name and the ID of the collection are right
	if c.Name != "testCol" {
		t.Error("collection has a bad name")
	}
	if c.ID != "4877b478786bce75b2cacf3c075ccfb2" {
		t.Error("collection has a bad ID")
	}

	if err := c.Put("testID", nil); err != nil {
		t.Error(err)
		return
	}
}

func TestPutGetAndDeleteObjectCollection(t *testing.T) {
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

	retrieveTestUser := struct {
		Login, Pass string
	}{}
	if getErr := c.Get("testID", &retrieveTestUser); getErr != nil {
		t.Error(getErr)
		return
	}

	if !reflect.DeepEqual(testUser, retrieveTestUser) {
		t.Error("given object and retreive on are not equal")
		return
	}
}
func TestPutGetAndDeleteBinCollection(t *testing.T) {
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

	c, useErr := db.Use("testCol")
	if useErr != nil {
		t.Error(useErr)
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

	db, openDBerr = Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()

	c, useErr = db.Use("testCol")
	if useErr != nil {
		t.Error(useErr)
		return
	}
	fmt.Println("FINI")

	// Test that the name and the ID of the collection are right
	if c.Name != "testCol" {
		t.Error("collection has a bad name")
	}
	if c.ID != "4877b478786bce75b2cacf3c075ccfb2" {
		t.Error("collection has a bad ID")
	}

	retrieveTestUser := struct {
		Login, Pass string
	}{}
	if getErr := c.Get("testID", &retrieveTestUser); getErr != nil {
		t.Error(getErr)
		return
	}

	if !reflect.DeepEqual(testUser, retrieveTestUser) {
		t.Error("given object and retreive on are not equal")
		return
	}
}
