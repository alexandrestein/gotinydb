package gotinydb

import (
	"context"
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

type (
	User struct {
		ID        string
		Email     string
		Balance   int
		Address   *Address
		Age       uint
		LastLogin time.Time
	}
	Address struct {
		City    string
		ZipCode uint
	}
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

	db, _ := fillUpDB(ctx, t, dataSet1)
	if db == nil {
		return
	}
	defer db.Close()
	defer os.RemoveAll(db.options.Path)

	testPath := db.options.Path

	if err := db.Close(); err != nil {
		t.Error(err)
		return
	}

	var err error
	db, err = Open(ctx, NewDefaultTransactionTimeOut(testPath))
	if err != nil {
		t.Error(err)
		return
	}

	c, userDBErr := db.Use("testCol")
	if userDBErr != nil {
		t.Error(err)
		return
	}

	response, queryErr := c.Query(NewQuery().SetFilter(NewFilter(Equal).SetSelector("Email").CompareTo("jonas-90@tlaloc.com")))
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
	db, openDBErr := Open(ctx, NewDefaultTransactionTimeOut(testPath))
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
	if c.name != "testCol" {
		t.Errorf("collection has a bad name %q", c.name)
	}
	if c.id != "SHe0eHhrznWyys88B1zPsg" {
		t.Errorf("collection has a bad ID %q", c.id)
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

	if !testPutGetAndDeleteCollectionFillupTestAndClose(ctx, testPath, t, userID, user, bin) {
		return
	}

	db, openDBErr := Open(ctx, NewDefaultTransactionTimeOut(testPath))
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
	if _, getErr := c.Get(userID, nil); getErr != ErrNotFound {
		t.Errorf("No error but the ID has been deleted")
		return
	}
}

func testPutGetAndDeleteCollectionFillupTestAndClose(ctx context.Context, testPath string, t *testing.T, userID string, user interface{}, bin bool) bool {
	db, openDBErr := Open(ctx, NewDefaultTransactionTimeOut(testPath))
	if openDBErr != nil {
		t.Error(openDBErr)
		return false
	}
	defer db.Close()

	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return false
	}

	if err := c.Put(userID, user); err != nil {
		t.Error(err)
		return false
	}

	if !bin {
		retrievedTestUser := struct {
			Login, Pass string
		}{}
		if _, getErr := c.Get(userID, &retrievedTestUser); getErr != nil {
			t.Error(getErr)
			return false
		}
		if !reflect.DeepEqual(user, retrievedTestUser) {
			t.Error("given object and retrieve on are not equal")
			return false
		}
	} else {
		retrieveContent, getErr := c.Get(userID, nil)
		if getErr != nil {
			t.Error(getErr)
			return false
		}
		if !reflect.DeepEqual(retrieveContent, user) {
			t.Error("given object and retrieve on are not equal")
			return false
		}
	}

	if err := db.Close(); err != nil {
		t.Error(err)
		return false
	}

	return true
}
