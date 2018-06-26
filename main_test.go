package gotinydb

import (
	"context"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(ctx, testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
		return
	}

	if err := db.Close(); err != nil {
		t.Error(err)
		return
	}
}

func TestCreateCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(ctx, testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(ctx, testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
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

	retrievedTestUser := struct {
		Login, Pass string
	}{}
	if _, getErr := c.Get("testID", &retrievedTestUser); getErr != nil {
		t.Error(getErr)
		return
	}

	if !reflect.DeepEqual(testUser, retrievedTestUser) {
		t.Error("given object and retrieve on are not equal")
		return
	}

	if delErr := c.Delete("testID"); delErr != nil {
		t.Error(delErr)
		return
	}

	retrievedTestUser = struct {
		Login, Pass string
	}{}

	if _, getErr := c.Get("testID", &retrievedTestUser); getErr == nil {
		t.Errorf("No error but the ID has been deleted")
	} else if getErr != vars.ErrNotFound {
		t.Error(getErr)
	}
}

func TestPutGetAndDeleteBinCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(ctx, testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
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

	retrieveContent, getErr := c.Get("testID", nil)
	if getErr != nil {
		t.Error(getErr)
		return
	}

	if !reflect.DeepEqual(retrieveContent, content) {
		t.Error("given object and retrieve on are not equal")
		return
	}

	if delErr := c.Delete("testID"); delErr != nil {
		t.Error(delErr)
		return
	}

	retrieveContent, getErr = c.Get("testID", nil)
	if getErr != vars.ErrNotFound {
		t.Error(getErr)
		return
	}
}
