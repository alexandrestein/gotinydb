package gotinydb

import (
	"encoding/json"
	"time"
)

type (
	User struct {
		ID        string
		Email     string `json:"email"`
		Balance   int
		Address   *Address
		Age       uint
		LastLogin time.Time
	}
	Address struct {
		City    string
		ZipCode uint
	}

	dataset []byte
)

func unmarshalDataset(ds dataset) (users []*User) {
	json.Unmarshal([]byte(ds), &users)
	return
}
