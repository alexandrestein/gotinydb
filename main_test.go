package gotinydb

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
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
		t.Errorf("collection has a bad name %q", c.Name)
	}
	if c.ID != "SHe0eHhrznWyys88B1zPsg" {
		t.Errorf("collection has a bad ID %q", c.ID)
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

	if delErr := c.Delete("testID"); delErr != nil {
		t.Error(delErr)
		return
	}

	retrieveTestUser = struct {
		Login, Pass string
	}{}
	if getErr := c.Get("testID", &retrieveTestUser); getErr != vars.ErrNotFound {
		t.Error(getErr)
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

	content := make([]byte, 1000)
	if _, readRandErr := rand.Read(content); readRandErr != nil {
		t.Error(readRandErr)
		return
	}

	if err := c.Put("testID", content); err != nil {
		t.Error(err)
		return
	}

	retrieveContent := bytes.NewBuffer(nil)
	if getErr := c.Get("testID", retrieveContent); getErr != nil {
		t.Error(getErr)
		return
	}

	if !reflect.DeepEqual(retrieveContent.Bytes(), content) {
		t.Error("given object and retreive on are not equal")
		return
	}

	if delErr := c.Delete("testID"); delErr != nil {
		t.Error(delErr)
		return
	}

	retrieveContent = bytes.NewBuffer(nil)
	if getErr := c.Get("testID", retrieveContent); getErr != vars.ErrNotFound {
		t.Error(getErr)
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

	// Test that the name and the ID of the collection are right
	if c.Name != "testCol" {
		t.Errorf("collection has a bad name %q", c.Name)
	}
	if c.ID != "SHe0eHhrznWyys88B1zPsg" {
		t.Errorf("collection has a bad ID %q", c.ID)
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
