package gotinydb

import (
	"time"
)

type (
	User struct {
		ID        string
		Email     string
		Balance   int64
		Address   *Address
		Age       uint8
		LastLogin time.Time
		PublicKey []byte
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
