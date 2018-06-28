package gotinydb

import (
	"time"
)

type (
	User struct {
		ID        string
		Email     string
		Balance   int
		Address   *Address
		Age       uint
		LastLogin time.Time
	}
	Address struct {
		City    string
		ZipCode uint
	}

	testListOfQueries struct {
		gets        []*testFilters
		limit       int
		wantedValue *User
	}

	testFilters struct {
		t         FilterOperator
		valToComp interface{}
		selector  []string
		limit     int
	}
)
