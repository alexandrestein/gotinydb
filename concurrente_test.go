package gotinydb

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
)

func TestConcurrentCollections(t *testing.T) {
	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
		return
	}
	defer db.Close()

	c1, userDBErr1 := db.Use("testCol1")
	if userDBErr1 != nil {
		t.Error(userDBErr1)
		return
	}
	c2, userDBErr2 := db.Use("testCol2")
	if userDBErr2 != nil {
		t.Error(userDBErr2)
		return
	}
	c3, userDBErr3 := db.Use("testCol3")
	if userDBErr3 != nil {
		t.Error(userDBErr3)
		return
	}

	doneChan := make(chan error, 3)
	go insertObjectsForConcurrent(c1, dataSet1, doneChan)
	go insertObjectsForConcurrent(c2, dataSet2, doneChan)
	go insertObjectsForConcurrent(c3, dataSet3, doneChan)

	for index := 0; index < 3; index++ {
		err := <-doneChan
		if err != nil {
			t.Error(err)
			return
		}
	}

	go checkObjectsForConcurrent(c1, dataSet1, doneChan)
	go checkObjectsForConcurrent(c2, dataSet2, doneChan)
	go checkObjectsForConcurrent(c3, dataSet3, doneChan)

	for index := 0; index < 3; index++ {
		err := <-doneChan
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func insertObjectsForConcurrent(c *Collection, dataSet []byte, done chan error) {
	users := unmarshalDataSet(dataSet)

	indexes := []struct {
		name     string
		selector []string
		t        vars.IndexType
	}{
		{"email", []string{"Email"}, vars.StringIndex},
		{"balance", []string{"Balance"}, vars.IntIndex},
		{"city", []string{"Address", "City"}, vars.StringIndex},
		{"zip", []string{"Address", "ZipCode"}, vars.IntIndex},
		{"age", []string{"Age"}, vars.IntIndex},
		{"last login", []string{"LastLogin"}, vars.TimeIndex},
	}

	for _, indexParams := range indexes {
		index := new(Index)
		index.Name = indexParams.name
		index.Selector = indexParams.selector
		index.Type = indexParams.t
		c.SetIndex(index)
	}

	for _, user := range users {
		if err := c.Put(user.ID, user); err != nil {
			done <- err
			return
		}
	}

	done <- nil
	return
}

func checkObjectsForConcurrent(c *Collection, dataSet []byte, done chan error) {
	users := unmarshalDataSet(dataSet)

	for _, user := range users {
		retrievedUser := new(User)
		if err := c.Get(user.ID, retrievedUser); err != nil {
			done <- err
			return
		}

		if !reflect.DeepEqual(user, retrievedUser) {
			done <- fmt.Errorf("the tow objects are not equal: \n%v\n%v", user, retrievedUser)
			return
		}
	}

	done <- nil
}

func unmarshalDataSet(dataSet []byte) []*User {
	users := []*User{}
	json.Unmarshal(dataSet, &users)
	return users
}

type (
	User struct {
		ID        string
		Email     string
		Balance   int64
		Address   *Address
		Age       uint8
		LastLogin time.Time
	}
	Address struct {
		City    string
		ZipCode uint
	}
)
