package gotinydb

import (
	"bytes"
	"os"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve"
)

var (
	testDB  *DB
	testCol *Collection

	testPath      = os.TempDir() + "/testDB"
	testConfigKey = [32]byte{}

	testColName = "collection name"

	testUserID = "test ID"
	testUser   = &user{"toto", "toto@internet.org"}

	testIndexName     = "email"
	testIndexSelector = "Email"
)

type (
	user struct {
		Name, Email string
	}
)

func init() {
	os.RemoveAll(testPath)
}

func TestMain(t *testing.T) {
	defer clean()
	err := open(t)
	if err != nil {
		return
	}

	retrievedUser := new(user)
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
	searchRequest := bleve.NewSearchRequestOptions(query, 10, 0, true)
	var searchResult *SearchResult
	searchResult, err = testCol.Search(testIndexName, searchRequest)
	if err != nil {
		t.Error(err)
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

	query = bleve.NewQueryStringQuery(testUser.Email)
	searchRequest = bleve.NewSearchRequestOptions(query, 10, 0, true)
	searchResult, err = testCol.Search(testIndexName, searchRequest)
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

	query = bleve.NewQueryStringQuery(testUser.Email)
	searchRequest = bleve.NewSearchRequestOptions(query, 10, 0, true)
	searchResult, err = testCol.Search(testIndexName, searchRequest)
	if err == nil {
		t.Errorf("the index should returns no result but had %s", searchResult.BleveSearchResult.String())
		return
	}
}

func open(t *testing.T) (err error) {
	testDB, err = Open(testPath, testConfigKey)
	if err != nil {
		t.Error(err)
		return err
	}

	testCol, err = testDB.Use(testColName)
	if err != nil {
		t.Error(err)
		return err
	}

	err = testCol.SetBleveIndex(testIndexName, bleve.NewIndexMapping(), testIndexSelector)
	if err != nil {
		t.Error(err)
		return err
	}

	err = testCol.Put(testUserID, testUser)
	if err != nil {
		t.Error(err)
		return err
	}

	return
}

func clean() {
	testDB.Close()
	os.RemoveAll(testPath)
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
	searchRequest := bleve.NewSearchRequestOptions(query, 10, 0, true)
	var searchResult *SearchResult
	searchResult, err = col2.Search(testIndexName, searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log("searchResult", searchResult)
	}
}
