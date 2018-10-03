package gotinydb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/blevesearch/bleve"
)

var (
	colName = "first collection name"
)

func TestMain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "mainTest"
	defer os.RemoveAll(testPath)

	db, err := Open(ctx, NewDefaultOptions(testPath))
	if err != nil {
		t.Fatal(err)
	}

	var col *Collection
	col, err = db.Use(colName)
	if err != nil {
		t.Fatal(err)
	}

	err = col.SetBleveIndex("email", bleve.NewIndexMapping(), "email")
	if err != nil {
		t.Fatal(err)
	}

	err = col.Put(testUser.ID, testUser)
	if err != nil {
		t.Fatal(err)
	}

	query := bleve.NewQueryStringQuery("clement")
	searchRequest := bleve.NewSearchRequest(query)
	var searchResult *SearchResult
	searchResult, err = col.Search("email", searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	retrievedUser := new(User)
	// var match *search.DocumentMatch
	_, err = searchResult.Next(retrievedUser)
	// match, err = searchResult.Next(retrievedUser)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("retrievedUser", retrievedUser)
}
