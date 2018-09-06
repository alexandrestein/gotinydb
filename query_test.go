package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCollection_Query(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "queryTest"
	defer os.RemoveAll(testPath)

	db, err := Open(ctx, NewDefaultOptions(testPath))
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	c, err := db.Use("user collection")
	if err != nil {
		t.Error(err)
		return
	}

	c.SetIndex("email", StringIndex, "email")
	c.SetIndex("age", IntIndex, "Age")
	c.SetIndex("last connection", TimeIndex, "history", "lastConnection")

	// Insert element in concurrent way to test the index system
	for _, dataset := range []dataset{dataset1, dataset2, dataset3} {
		var wg sync.WaitGroup
		for _, user := range unmarshalDataset(dataset) {
			wg.Add(1)
			go func(c *Collection, user *User) {
				err := c.Put(user.ID, user)
				if err != nil {
					fmt.Println("err", err)
					t.Error(err)
					return
				}
				wg.Done()
			}(c, user)
		}

		fmt.Println("wait")
		wg.Wait()
		fmt.Println("new loop")
	}

	tests := []struct {
		name         string
		args         *Query
		wantResponse []*User
		wantErr      bool
	}{
		{
			name: "Equal String Limit 1",
			args: NewQuery().SetFilter(
				NewFilter(Equal).CompareTo("estrada-21@allie.com").SetSelector("email"),
			).SetLimits(1, 0),
			wantResponse: []*User{
				{ID: "13", Email: "estrada-21@allie.com", Balance: 2923864648279932937, Address: &Address{City: "Nellie", ZipCode: 83}, Age: 10, LastLogin: mustParseTime("2016-11-20T08:59:24.779282825+01:00")},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResponse, err := c.Query(tt.args)

			if (err != nil) != tt.wantErr {
				t.Errorf("Collection.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			users := make([]*User, gotResponse.Len())
			i := 0
			if _, err := gotResponse.All(func(id string, objAsBytes []byte) error {
				tmpObj := new(User)
				err := json.Unmarshal(objAsBytes, tmpObj)
				if err != nil {
					return err
				}
				// Add the element into the slice
				users[i] = tmpObj

				i++
				return nil
			}); err != nil {
				t.Error(err)
				return
			}

			if !reflect.DeepEqual(users, tt.wantResponse) {
				t.Errorf("\n%v\n%v", printSliceOfUsers(users), printSliceOfUsers(tt.wantResponse))
			}
		})
	}
}

func mustParseTime(input string) time.Time {
	t, _ := time.Parse(time.RFC3339, input)
	return t
}

func printSliceOfUsers(input []*User) (ret string) {
	for i, user := range input {
		ret = fmt.Sprintf("%s%d\t%s\n", ret, i, user.String())
	}
	return
}
