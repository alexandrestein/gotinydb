package main

import (
	"context"
	"fmt"
	"time"

	"github.com/alexandrestein/gotinydb"
)

type (
	user struct {
		ID        string
		Email     string
		Balance   int
		Address   *address
		Age       uint
		LastLogin time.Time
	}
	address struct {
		City    string
		ZipCode uint
	}
)

// Basic open a new or existing database.
// Build one string index.
// Insert one element.
// And query the database to get this element.
func Basic() error {
	// getTestPathChan is an test channel to get test path to TMP directory
	dbTestPath := "basicExample"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, openDBErr := gotinydb.Open(ctx,
		gotinydb.NewDefaultOptions(dbTestPath),
	)

	if openDBErr != nil {
		return openDBErr
	}
	defer db.Close()

	// Open a collection
	c, useDBErr := db.Use("users")
	if useDBErr != nil {
		return useDBErr
	}

	// Setup indexexes
	indexErr := c.SetIndex("email", gotinydb.StringIndex, "Email")
	if indexErr != nil {
		if indexErr.Error() != "bucket already exists" {
			return indexErr
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

	// Initialize a query pointer
	queryPointer := gotinydb.NewQuery()

	// Build the filter
	queryFilter := gotinydb.NewFilter(gotinydb.Equal).
		SetSelector("Email").
		CompareTo("jonas-90@tlaloc.com")

	// Add the filter to the query pointer
	queryPointer.SetFilter(queryFilter)

	// Or this could be:
	queryPointer = gotinydb.NewQuery().SetFilter(
		gotinydb.NewFilter(gotinydb.Equal).
			SetSelector("Email").
			CompareTo("jonas-90@tlaloc.com"),
	)

	// Query the collection to get the struct based on the Email field
	response, queryErr := c.Query(queryPointer)
	if queryErr != nil {
		return queryErr
	}

	// Convert the reccored into a struct using JSON internally
	retrievedRecord := new(user)
	id, respErr := response.One(retrievedRecord)
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
