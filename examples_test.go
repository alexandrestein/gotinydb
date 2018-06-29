package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
)

type (
	Type struct{}
)

var (
	responseQuery *ResponseQuery
	result        = []*Type{}
)

func Example() {
	// getTestPathChan is an test channel to get test path to TMP directory
	dbTestPath := <-getTestPathChan

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, openDBErr := Open(ctx, dbTestPath)
	if openDBErr != nil {
		log.Fatal(openDBErr)
		return
	}
	defer db.Close()
	// Clean database directory after the test
	defer os.RemoveAll(dbTestPath)

	// Open a collection
	c, useDBErr := db.Use("testCol")
	if useDBErr != nil {
		log.Println(useDBErr)
		return
	}

	// Setup indexexes
	c.SetIndex(NewIndex("email", vars.StringIndex, "Email"))
	c.SetIndex(NewIndex("projects counter", vars.StringIndex, "NbProject"))
	c.SetIndex(NewIndex("last login", vars.StringIndex, "LastLogin"))

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
		log.Println(err)
		return
	}

	// Query the collection to get the struct based on the Email field
	response, queryErr := c.Query(NewQuery().SetFilter(NewFilter(Equal).SetSelector("Email").CompareTo("jonas-90@tlaloc.com")))
	if queryErr != nil {
		log.Println(queryErr)
		return
	}

	// Convert the reccored into a struct using JSON internally
	retrievedRecord := new(User)
	id, respErr := response.One(retrievedRecord)
	if respErr != nil {
		log.Println(respErr)
		return
	}

	// Display the result
	fmt.Println(id, retrievedRecord)

	// Output: id &{ jonas-90@tlaloc.com 0 <nil> 0 0001-01-01 00:00:00 +0000 UTC}
}

func ExampleResponseQuery_All() {
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

func ExampleResponseQuery_Next() {
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
func ExampleResponseQuery_First() {
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

func ExampleResponseQuery_Prev() {
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
func ExampleResponseQuery_Last() {
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

func ExampleResponseQuery_One() {
	for i := 0; true; i++ {
		tmpObj := new(Type)
		_, err := responseQuery.One(tmpObj)
		if err != nil {
			if err == vars.ErrTheResponseIsOver {
				break
			}
			// Handler error
		}
		result[i] = tmpObj
	}
	// Slice is filled up your code goes here
}
