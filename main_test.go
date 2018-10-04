package gotinydb

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
)

var (
	db      *DB
	col     *Collection
	colName = "first collection name"

	testPath = os.TempDir() + "/testDB"
)

func TestMain(t *testing.T) {
	defer clean()
	buildBaseDB(t)

	query := bleve.NewFuzzyQuery("cindy")
	searchRequest := bleve.NewSearchRequest(query)
	searchResult, err := col.Search("email", searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	retrievedUser := new(User)
	_, err = searchResult.Next(retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log(retrievedUser)
	}
}

func buildBaseDB(t *testing.T) {
	var err error
	opt := NewDefaultOptions(testPath)
	opt.TransactionTimeOut = time.Minute * 10
	db, err = Open(context.Background(), opt)
	if err != nil {
		t.Error(err)
		return
	}

	col, err = db.Use(colName)
	if err != nil {
		t.Error(err)
		return
	}

	err = col.SetBleveIndex("email", bleve.NewIndexMapping(), "email")
	if err != nil {
		t.Error(err)
		return
	}

	users1 := unmarshalDataset(dataset1)
	users2 := unmarshalDataset(dataset2)
	users3 := unmarshalDataset(dataset3)

	var wg sync.WaitGroup
	for i := range users1 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := col.Put(users1[i].ID, users1[i])
			if err != nil {
				t.Error(err)
				return
			}
			err = col.Put(users2[i].ID, users2[i])
			if err != nil {
				t.Error(err)
				return
			}
			err = col.Put(users3[i].ID, users3[i])
			if err != nil {
				t.Error(err)
				return
			}
		}(i)
	}

	wg.Wait()
}

func clean() {
	db.Close()
	os.RemoveAll(testPath)
}

func TestSetIndexDataPresent(t *testing.T) {
	defer clean()
	buildBaseDB(t)

	err := col.SetBleveIndex("age", bleve.NewIndexMapping(), "Age")
	if err != nil {
		t.Error(err)
		return
	}

	valueToTest := 15.0
	include := true
	query := bleve.NewNumericRangeInclusiveQuery(&valueToTest, &valueToTest, &include, &include)
	searchRequest := bleve.NewSearchRequest(query)
	var searchResult *SearchResult
	searchResult, err = col.Search("age", searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log(searchResult)
	}
}

func TestIndexAllObject(t *testing.T) {
	defer clean()
	buildBaseDB(t)

	err := col.SetBleveIndex("all", bleve.NewIndexMapping(), "")
	if err != nil {
		t.Error(err)
		return
	}

	valueToTest := 15.0
	include := true
	query := bleve.NewNumericRangeInclusiveQuery(&valueToTest, &valueToTest, &include, &include)
	searchRequest := bleve.NewSearchRequest(query)
	var searchResult *SearchResult
	searchResult, err = col.Search("all", searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log(searchResult)
	}
}

func TestFiles(t *testing.T) {
	defer clean()
	buildBaseDB(t)

	opt := NewDefaultOptions(testPath)
	// 235KB
	opt.FileChunkSize = 235 * 100

	opt.TransactionTimeOut = time.Minute * 10

	db.SetOptions(opt)

	// 100MB, it will be made 4256 chunks
	randBuff := make([]byte, 100*1000*1000)
	rand.Read(randBuff)

	fileID := "test file ID"

	n, err := db.PutFile(fileID, bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}

	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	randHash := blake2b.Sum256(randBuff)

	readBuff := bytes.NewBuffer(nil)
	err = db.ReadFile(fileID, readBuff)
	if err != nil {
		t.Error(err)
		return
	}

	readHash := blake2b.Sum256(readBuff.Bytes())

	if !reflect.DeepEqual(randHash, readHash) {
		t.Error("the saved file and the rand file are not equal")
		return
	}

	// Check the ids with chunk number are well generated
	err = db.badgerDB.View(func(txn *badger.Txn) error {
		storeID := db.buildFilePrefix(fileID, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		it := txn.NewIterator(opt)
		defer it.Close()
		prevLastByte := -1
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			lastByte := int(it.Item().Key()[len(it.Item().Key())-1:][0])
			if prevLastByte+1 != lastByte {
				if prevLastByte == 255 && lastByte != 0 {
					t.Errorf("generated incremental bytes is not good")
				}
			}
			prevLastByte = lastByte
		}

		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = db.DeleteFile(fileID)
	if err != nil {
		t.Error(err)
		return
	}

	err = db.badgerDB.View(func(txn *badger.Txn) error {
		storeID := db.buildFilePrefix(fileID, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			return fmt.Errorf("must be empty response")
		}

		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
}

func TestFilesMultipleWriteSameID(t *testing.T) {
	defer clean()
	buildBaseDB(t)

	// 20MB
	randBuff := make([]byte, 15*1000*1000)
	rand.Read(randBuff)

	fileID := "test file ID"

	n, err := db.PutFile(fileID, bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	// New smaller file 5MB
	randBuff = make([]byte, 5*1000*1000)
	rand.Read(randBuff)

	n, err = db.PutFile(fileID, bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}
	if n != len(randBuff) {
		t.Errorf("expected write size %d but had %d", len(randBuff), n)
		return
	}

	readBuff := bytes.NewBuffer(nil)
	err = db.ReadFile(fileID, readBuff)
	if err != nil {
		t.Error(err)
		return
	}

	randHash := blake2b.Sum256(randBuff)
	readHash := blake2b.Sum256(readBuff.Bytes())

	if !reflect.DeepEqual(randHash, readHash) {
		t.Error("the saved file and the rand file are not equal")
		return
	}
}

func TestCloseOpen(t *testing.T) {
	defer clean()
	buildBaseDB(t)

	err := db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	db, err = Open(ctx, NewDefaultOptions(testPath))
	if err != nil {
		t.Error(err)
		return
	}

	col, err = db.Use(colName)
	if err != nil {
		t.Error(err)
		return
	}

	query := bleve.NewFuzzyQuery("cindy")
	searchRequest := bleve.NewSearchRequest(query)
	var searchResult *SearchResult
	searchResult, err = col.Search("email", searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	retrievedUser := new(User)
	_, err = searchResult.Next(retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log(retrievedUser)
	}
}

func TestBackup(t *testing.T) {
	defer clean()
	buildBaseDB(t)

	var backup bytes.Buffer

	_, err := db.Backup(&backup, 0)
	if err != nil {
		t.Error(err)
		return
	}

	restoredDBPath := os.TempDir() + "/restoredDB"
	defer os.RemoveAll(restoredDBPath)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	var restoredDB *DB
	restoredDB, err = Open(ctx, NewDefaultOptions(restoredDBPath))
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
	col2, err = restoredDB.Use(colName)
	if err != nil {
		t.Error(err)
		return
	}

	query := bleve.NewFuzzyQuery("cindy")
	searchRequest := bleve.NewSearchRequest(query)
	var searchResult *SearchResult
	searchResult, err = col2.Search("email", searchRequest)
	if err != nil {
		t.Error(err)
		searchResult, err = col.Search("email", searchRequest)
		fmt.Println(searchResult)
		return
	}

	retrievedUser := new(User)
	_, err = searchResult.Next(retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log(retrievedUser)
	}
}
