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
	dbTestPath := os.TempDir() + "/basicExample"
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
	indexErr := c.SetIndex("email", StringIndex, "email")
	if indexErr != nil {
		log.Print(indexErr)
		return
	}

	// Example struct
	record := &User{
		Email:     "jonas-90@tlaloc.com",
		ID:        "316",
		LastLogin: time.Time{},
	}

	// Save it in DB
	if err := c.Put(record.ID, record); err != nil {
		log.Print(err)
		return
	}

	// Initialize a query pointer
	queryPointer := c.NewQuery()

	// Build the filter
	queryFilter := NewEqualFilter("jonas-90@tlaloc.com", "email")

	// Add the filter to the query pointer
	queryPointer.SetFilter(queryFilter)

	// Or this could be:
	queryPointer = c.NewQuery().SetFilter(
		NewEqualFilter("jonas-90@tlaloc.com", "email"),
	)

	// Query the collection to get the struct based on the email field
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

	// Output: 316 {"ID":"316","email":"jonas-90@tlaloc.com","Balance":0,"Address":null,"Age":0,"LastLogin":"0001-01-01T00:00:00Z"}
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
			if err == ErrResponseOver {
				break
			}
			// Handler error
		}
		result[i] = tmpObj
	}
	// Slice is filled up your code goes here
}

func ExampleCollection_NewQuery() {
	// Build a new query
	q := collection.NewQuery().SetFilter(
		NewEqualFilter("jonas-90@tlaloc.com", "email"),
	)

	// Initialize an struct to get the value using One method
	recordReceiver := struct{ Email string }{}
	response, _ := collection.Query(q)

	// recordReceiver should be filled up with the recorded data
	response.One(recordReceiver)
}
