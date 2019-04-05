// +build !race

package gotinydb

import (
	"bytes"
	"os"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve"
)

func TestMain(t *testing.T) {
	defer clean()
	err := open(t)
	if err != nil {
		return
	}

	retrievedUser := new(testUserStruct)
	_, err = testCol.Get(testUserID, retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(retrievedUser, testUser) {
		t.Errorf("the users are not equal. Put %v and get %v", testUser, retrievedUser)
		return
	}

	query := bleve.NewQueryStringQuery(testUser.Email)
	var searchResult *SearchResult
	searchResult, err = testCol.Search(testIndexName, query)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log("searchResult", searchResult)
	}

	query = bleve.NewQueryStringQuery(testUser.Name)
	searchResult, err = testCol.Search(testIndexName, query)
	if err == nil {
		t.Errorf("the index must return no result but had %s", searchResult.BleveSearchResult.String())
		return
	}

	if testing.Verbose() {
		t.Log("searchResult", searchResult)
	}

	err = testDB.Close()
	if err != nil {
		t.Error(err)
		return
	}

	testDB = nil
	testCol = nil

	testDB, err = Open(testPath, testConfigKey)
	if err != nil {
		t.Error(err)
		return
	}

	testCol, err = testDB.Use(testColName)
	if err != nil {
		t.Error(err)
		return
	}

	// test get
	retrievedUser = new(testUserStruct)
	_, err = testCol.Get(testUserID, retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(testUser, retrievedUser) {
		t.Errorf("the expected object is %v and got %v", testUser, retrievedUser)
		return
	}

	// test get multi
	ids := []string{testUserID, cloneTestUserID}
	destinations := []interface{}{new(testUserStruct), new(testUserStruct)}
	_, err = testCol.GetMulti(ids, destinations)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(testUser, destinations[0]) {
		t.Errorf("the expected object is %v and got %v", testUser, destinations[0])
		return
	}
	if !reflect.DeepEqual(cloneTestUser, destinations[1]) {
		t.Errorf("the expected object is %v and got %v", cloneTestUser, destinations[1])
		return
	}

	query = bleve.NewQueryStringQuery(testUser.Email)
	searchResult, err = testCol.Search(testIndexName, query)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log("searchResult", searchResult)
	}

	err = testCol.Delete(testUserID)
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.Delete(cloneTestUserID)
	if err != nil {
		t.Error(err)
		return
	}

	query = bleve.NewQueryStringQuery(testUser.Email)
	searchResult, err = testCol.Search(testIndexName, query)
	if err == nil {
		t.Errorf("the index should returns no result but had %s", searchResult.BleveSearchResult.String())
		return
	}
}

func TestBackup(t *testing.T) {
	defer clean()
	err := open(t)
	if err != nil {
		return
	}

	var backup bytes.Buffer

	err = testDB.Backup(&backup)
	if err != nil {
		t.Error(err)
		return
	}

	restoredDBPath := os.TempDir() + "/restoredDB"
	defer os.RemoveAll(restoredDBPath)

	var restoredDB *DB
	restoredDB, err = Open(restoredDBPath, testConfigKey)
	if err != nil {
		t.Error(err)
		return
	}

	err = restoredDB.Load(&backup)
	if err != nil {
		t.Error(err)
		return
	}

	var col2 *Collection
	col2, err = restoredDB.Use(testColName)
	if err != nil {
		t.Error(err)
		return
	}

	query := bleve.NewQueryStringQuery(testUser.Email)
	var searchResult *SearchResult
	searchResult, err = col2.Search(testIndexName, query)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log("searchResult", searchResult)
	}
}

func TestChainOpen(t *testing.T) {
	db, err := Open("db", testConfigKey)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var userCollection *Collection
	userCollection, err = db.Use("users")
	if err != nil {
		t.Fatal(err)
	}

	err = userCollection.SetBleveIndex("all", bleve.NewDocumentMapping())
	if err != nil {
		t.Fatal(err)
	}
}
func TestChainReOpen(t *testing.T) {
	defer os.RemoveAll("db")
	db, err := Open("db", testConfigKey)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var userCollection *Collection
	userCollection, err = db.Use("users")
	if err != nil {
		t.Fatal(err)
	}

	err = userCollection.SetBleveIndex("all", bleve.NewDocumentMapping())
	if err == nil {
		t.Fatal("the index exist")
	}
}
