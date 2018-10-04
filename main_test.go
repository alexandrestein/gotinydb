package gotinydb

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/blevesearch/bleve"
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

	err := col.Put(testUser.ID, testUser)
	if err != nil {
		t.Error(err)
		return
	}

	query := bleve.NewQueryStringQuery("cindy")
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

func buildBaseDB(t *testing.T) {
	var err error
	db, err = Open(context.Background(), NewDefaultOptions(testPath))
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

