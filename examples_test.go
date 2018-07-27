package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type (
	Type struct{}
)

var (
	responseQuery *Response
	result        = []*Type{}
	collection    *Collection
)

func Example() {
	// getTestPathChan is an test channel to get test path to TMP directory
	dbTestPath := "basicExample"
	defer os.RemoveAll(dbTestPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, openDBErr := Open(ctx,
		NewDefaultOptions(dbTestPath),
	)

	if openDBErr != nil {
		log.Print(openDBErr)
		return
	}
	defer db.Close()

	// Open a collection
	c, useDBErr := db.Use("users")
	if useDBErr != nil {
		log.Print(useDBErr)
		return
	}

	// Setup indexexes
	indexErr := c.SetIndex("email", StringIndex, "Email")
	if indexErr != nil {
		log.Print(indexErr)
		return
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
		log.Print(err)
		return
	}

	// Initialize a query pointer
	queryPointer := NewQuery()

	// Build the filter
	queryFilter := NewFilter(Equal).
		SetSelector("Email").
		CompareTo("jonas-90@tlaloc.com")

	// Add the filter to the query pointer
	queryPointer.SetFilter(queryFilter)

	// Or this could be:
	queryPointer = NewQuery().SetFilter(
		NewFilter(Equal).
			SetSelector("Email").
			CompareTo("jonas-90@tlaloc.com"),
	)

	// Query the collection to get the struct based on the Email field
	response, queryErr := c.Query(queryPointer)
	if queryErr != nil {
		log.Print(queryErr)
		return
	}

	// Convert the reccored into a struct using JSON internally
	retrievedRecord := new(User)
	id, respErr := response.One(retrievedRecord)
	if respErr != nil {
		log.Print(respErr)
		return
	}

	// Display the result
	fmt.Println(id, retrievedRecord)

	// Output: id &{ jonas-90@tlaloc.com 0 <nil> 0 0001-01-01 00:00:00 +0000 UTC}
}

func ExampleResponse_All() {
	i := 0
	if _, err := responseQuery.All(func(id string, objAsBytes []byte) error {
		tmpObj := new(Type)
		err := json.Unmarshal(objAsBytes, tmpObj)
		if err != nil {
			return err
		}
		// Add the element into the slice
		result[i] = tmpObj

		i++
		return nil
	}); err != nil {
		// Handler error
	}
}

func ExampleResponse_Next() {
	for i, _, v := responseQuery.First(); i >= 0; i, _, v = responseQuery.Next() {
		tmpObj := new(Type)
		err := json.Unmarshal(v, tmpObj)
		if err != nil {
			// Handler error
		}

		result[i] = tmpObj
	}
	// Slice is filled up your code goes here
}
func ExampleResponse_First() {
	for i, _, v := responseQuery.First(); i >= 0; i, _, v = responseQuery.Next() {
		tmpObj := new(Type)
		err := json.Unmarshal(v, tmpObj)
		if err != nil {
			// Handler error
		}

		result[i] = tmpObj
	}
	// Slice is filled up your code goes here
}

func ExampleResponse_Prev() {
	for i, _, v := responseQuery.Last(); i >= 0; i, _, v = responseQuery.Prev() {
		tmpObj := new(Type)
		err := json.Unmarshal(v, tmpObj)
		if err != nil {
			// Handler error
		}

		result[i] = tmpObj
	}
	// Slice is filled up your code goes here
}
func ExampleResponse_Last() {
	// List all result from the last to the last with the prev function
	for i, _, v := responseQuery.Last(); i >= 0; i, _, v = responseQuery.Prev() {
		tmpObj := new(Type)
		err := json.Unmarshal(v, tmpObj)
		if err != nil {
			// Handler error
		}

		result[i] = tmpObj
	}
	// Slice is filled up your code goes here
}

func ExampleResponse_One() {
	for i := 0; true; i++ {
		tmpObj := new(Type)
		_, err := responseQuery.One(tmpObj)
		if err != nil {
			if err == ErrTheResponseIsOver {
				break
			}
			// Handler error
		}
		result[i] = tmpObj
	}
	// Slice is filled up your code goes here
}

func ExampleNewQuery() {
	// Build a new query
	q := NewQuery().SetFilter(
		NewFilter(Equal).SetSelector("Email").CompareTo("jonas-90@tlaloc.com"),
	)

	// Initialize an struct to get the value using One method
	recordReceiver := struct{ Email string }{}
	response, _ := collection.Query(q)

	// recordReceiver should be filled up with the recorded data
	response.One(recordReceiver)
}
