package main

import (
	"fmt"
	"os"
	"time"

	"github.com/alexandrestein/gotinydb"
	"github.com/blevesearch/bleve"
)

type (
	user struct {
		ID        string
		Email     string
		Balance   int
		Age       uint
		LastLogin time.Time
	}
)

// Type implements the github.com/blevesearch/bleve/mapping/#Classifier interface to have a easy
// way to check types in case you put multiple types in the same collection.
func (u *user) Type() string {
	return "exampleUser"
}

// Basic open a new or existing database.
// Build one string index.
// Insert one element.
// And query the database to get this element.
func Basic() error {
	// getTestPathChan is an test channel to get test path to TMP directory
	dbTestPath := os.TempDir() + "/basicExample"

	db, err := gotinydb.Open(dbTestPath, [32]byte{})
	if err != nil {
		return err
	}
	defer db.Close()

	// Open a collection
	var c *gotinydb.Collection
	c, err = db.Use("users")
	if err != nil {
		return err
	}

	// Build the index mapping
	//
	// Build a static mapping document to index only specified fields
	userDocumentMapping := bleve.NewDocumentStaticMapping()
	// Build the field checker
	emailFieldMapping := bleve.NewTextFieldMapping()
	// Add a text filed to Email property
	userDocumentMapping.AddFieldMappingsAt("Email", emailFieldMapping)
	// Build the index mapping it self
	indexMapping := bleve.NewIndexMapping()
	indexMapping.AddDocumentMapping("exampleUser", userDocumentMapping)

	// Setup indexexes
	err = c.SetBleveIndex("email", indexMapping)
	if err != nil {
		if err != gotinydb.ErrNameAllreadyExists {
			return err
		}
	}

	// Example struct
	record := struct {
		Email     string
		NbProject int
		LastLogin time.Time
	}{
		"jonas-90@tlaloc.com",
		316,
		time.Time{},
	}

	// Save it in DB
	if err := c.Put("id", record); err != nil {
		return err
	}

	// Build the query
	query := bleve.NewQueryStringQuery(record.Email)
	// Add the query to the search
	var searchResult *gotinydb.SearchResult
	searchResult, err = c.Search("email", query)
	if err != nil {
		return err
	}

	// Convert the reccored into a struct using JSON internally
	retrievedRecord := new(user)
	id, respErr := searchResult.Next(retrievedRecord)
	if respErr != nil {
		return respErr
	}

	// Display the result
	fmt.Println(id, retrievedRecord)

	// Output: id &{ jonas-90@tlaloc.com 0 <nil> 0 0001-01-01 00:00:00 +0000 UTC}

	return nil
}

func main() {
	if err := Basic(); err != nil {
		fmt.Println(err)
	}
}
