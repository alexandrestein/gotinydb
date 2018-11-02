package gotinydb

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"
)

var (
	testDB  *DB
	testCol *Collection

	testPath      = os.TempDir() + "/testDB"
	testConfigKey = [32]byte{}

	testColName = "collection name"

	testUserID = "test ID"
	testUser   = &testUserStruct{
		"toto",
		"userName@internet.org",
		&Account{"Github", "https://www.github.com"},
	}

	testIndexName = "email"
	// testIndexSelector         = "Email"
	testIndexNameAccounts = "accounts"
	// testIndexSelectorAccounts = []string{"accounts", "Name"}
)

type (
	testUserStruct struct {
		Name  string   `json:"name"`
		Email string   `json:"email"`
		Oauth *Account `json:"oauth"`
	}
	Account struct {
		Name, URL string
	}
)

func (t *testUserStruct) Type() string {
	return "local.testUserStruct"
}

func init() {
	tmpKey, err := base64.RawStdEncoding.DecodeString("/HpNPL+GzfLDsA642L7jdKcLuaGV8ijv9f9prSGRGIg")
	if err != nil {
		log.Fatal(err)
	}

	copy(testConfigKey[:], tmpKey[:])

	os.RemoveAll(testPath)
}

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

	query = bleve.NewQueryStringQuery(testUser.Email)
	searchResult, err = testCol.Search(testIndexName, query)
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

	userDocumentMapping := bleve.NewDocumentStaticMapping()

	emailFieldMapping := bleve.NewTextFieldMapping()
	userDocumentMapping.AddFieldMappingsAt("email", emailFieldMapping)

	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping(testUser.Type(), userDocumentMapping)

	err = testCol.SetBleveIndex(testIndexName, indexMapping)
	if err != nil {
		t.Error(err)
		return err
	}

	err = testCol.SetBleveIndex("all", bleve.NewIndexMapping())
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

func TestHistory(t *testing.T) {
	defer clean()
	open(t)

	testID := "the history test ID"
	testCol.Put(testID, []byte("value 10"))
	testCol.Put(testID, []byte("value 9"))
	testCol.Put(testID, []byte("value 8"))
	testCol.Put(testID, []byte("value 7"))
	testCol.Put(testID, []byte("value 6"))
	testCol.Put(testID, []byte("value 5"))
	testCol.Put(testID, []byte("value 4"))
	testCol.Put(testID, []byte("value 3"))
	testCol.Put(testID, []byte("value 2"))
	testCol.Put(testID, []byte("value 1"))
	testCol.Put(testID, []byte("value 0"))

	// Load part of the history
	valuesAsBytes, err := testCol.History(testID, 5)
	if err != nil {
		t.Error(err)
		return
	}
	for i, val := range valuesAsBytes {
		if fmt.Sprintf("value %d", i) != string(val) {
			t.Errorf("the history is not what is expected")
			return
		}
	}

	// Load more than the existing history
	valuesAsBytes, err = testCol.History(testID, 15)
	if err != nil {
		t.Error(err)
		return
	}
	for i, val := range valuesAsBytes {
		if fmt.Sprintf("value %d", i) != string(val) {
			t.Errorf("the history is not what is expected")
			return
		}
	}

	// Update the value with a fresh history
	freshHistoryValue := []byte("brand new element")
	err = testCol.PutWithCleanHistory(testID, freshHistoryValue)
	if err != nil {
		t.Error(err)
		return
	}

	valuesAsBytes, err = testCol.History(testID, 5)
	if err != nil {
		t.Error(err)
		return
	}

	if l := len(valuesAsBytes); l > 1 {
		t.Errorf("history returned more than 1 value %d", l)
		return
	}
	if string(valuesAsBytes[0]) != string(freshHistoryValue) {
		t.Errorf("the returned content from history is not correct")
	}
}

func TestDeleteParts(t *testing.T) {
	defer clean()
	open(t)

	bleveIndex, _ := testCol.GetBleveIndex(testIndexName)
	prefix := bleveIndex.Prefix
	testCol.DeleteIndex(testIndexName)

	time.Sleep(time.Second)

	testDB.badger.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		iter := txn.NewIterator(opt)
		defer iter.Close()

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			t.Errorf("this id must be deleted %v", iter.Item().Key())
		}

		return nil
	})
	_, err := testCol.GetBleveIndex(testIndexName)
	if err == nil {
		t.Errorf("the index is deleted")
		return
	}

	prefix = testCol.Prefix
	testDB.DeleteCollection(testColName)

	time.Sleep(time.Second)

	testDB.badger.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false
		iter := txn.NewIterator(opt)
		defer iter.Close()

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			t.Errorf("this id must be deleted %v", iter.Item().Key())
		}

		return nil
	})
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

	err = userCollection.SetBleveIndex("all", bleve.NewIndexMapping())
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

	err = userCollection.SetBleveIndex("all", bleve.NewIndexMapping())
	if err == nil {
		t.Fatal("the index exist")
	}
}
