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
		Related   []int `json:"related,omitempty"`
	}
	Address struct {
		City    string `json:"city"`
		ZipCode uint
	}

	dataset []byte
)

func (u *User) String() string {
	asBytes, _ := json.Marshal(u)
	return string(asBytes)
}

func unmarshalDataset(ds dataset) (users []*User) {
	json.Unmarshal([]byte(ds), &users)
	return
}
