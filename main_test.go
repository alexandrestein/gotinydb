package gotinydb

import (
	"context"
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

	db, _ := queryFillUp(ctx, t, dataSet1)
	if db == nil {
		return
	}
	defer db.Close()
	defer os.RemoveAll(db.Path)

	testPath := db.Path

	if err := db.Close(); err != nil {
		t.Error(err)
		return
	}

	var err error
	db, err = Open(ctx, testPath)
	if err != nil {
		t.Error(err)
		return
	}

	c, userDBErr := db.Use("testCol")
	if userDBErr != nil {
		t.Error(err)
		return
	}

	response, queryErr := c.Query(NewQuery().Get(NewFilter(Equal).SetSelector([]string{"Email"}).CompareTo("jonas-90@tlaloc.com")))
	if queryErr != nil {
		t.Error(queryErr)
		return
	}

	user := new(User)
	id, respErr := response.One(user)
	if respErr != nil {
		t.Error(respErr)
		return
	}

	if id != "0" {
		t.Errorf("%s is not the right ID. Expected %s", id, "0")
		return
	}

	if !reflect.DeepEqual(user, unmarshalDataSet(dataSet1)[0]) {
		t.Errorf("%v is not the right value. Expected %v", user, unmarshalDataSet(dataSet1)[0])
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
	testUser := struct {
		Login, Pass string
	}{"User 1", "super password"}

	testPutGetAndDeleteCollection(t, "id", testUser, false)
}

func TestPutGetAndDeleteBinCollection(t *testing.T) {
	content := make([]byte, 1000)
	testPutGetAndDeleteCollection(t, "id", content, true)
}

func testPutGetAndDeleteCollection(t *testing.T, userID string, user interface{}, bin bool) {
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

	if err := c.Put(userID, user); err != nil {
		t.Error(err)
		return
	}

	if !bin {
		retrievedTestUser := struct {
			Login, Pass string
		}{}
		if _, getErr := c.Get(userID, &retrievedTestUser); getErr != nil {
			t.Error(getErr)
			return
		}
		if !reflect.DeepEqual(user, retrievedTestUser) {
			t.Error("given object and retrieve on are not equal")
			return
		}
	} else {
		retrieveContent, getErr := c.Get(userID, nil)
		if getErr != nil {
			t.Error(getErr)
			return
		}
		if !reflect.DeepEqual(retrieveContent, user) {
			t.Error("given object and retrieve on are not equal")
			return
		}
	}

	if err := db.Close(); err != nil {
		t.Error(err)
		return
	}
	cancel()

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	db, openDBErr = Open(ctx, testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
		return
	}
	defer db.Close()

	c, userErr = db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	if !bin {
		retrievedTestUser := struct {
			Login, Pass string
		}{}
		if _, getErr := c.Get(userID, &retrievedTestUser); getErr != nil {
			t.Error(getErr)
			return
		}
		if !reflect.DeepEqual(user, retrievedTestUser) {
			t.Error("given object and retrieve on are not equal")
			return
		}
	} else {
		retrieveContent, getErr := c.Get(userID, nil)
		if getErr != nil {
			t.Error(getErr)
			return
		}
		if !reflect.DeepEqual(retrieveContent, user) {
			t.Error("given object and retrieve on are not equal")
			return
		}
	}

	if delErr := c.Delete(userID); delErr != nil {
		t.Error(delErr)
		return
	}

	if _, getErr := c.Get(userID, nil); getErr != vars.ErrNotFound {
		t.Errorf("No error but the ID has been deleted")
		return
	}
}
