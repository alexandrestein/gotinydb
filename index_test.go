package gotinydb

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/alexandrestein/gotinydb/vars"
)

func TestConcurrentCollections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

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

	if err := setIndexes(c1); err != nil {
		t.Error(err)
		return
	}
	if err := setIndexes(c2); err != nil {
		t.Error(err)
		return
	}
	if err := setIndexes(c3); err != nil {
		t.Error(err)
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

func setIndexes(c *Collection) error {
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
		if err := c.SetIndex(index); err != nil {
			return err
		}
	}
	return nil
}

func insertObjectsForConcurrent(c *Collection, dataSet []byte, done chan error) {
	users := unmarshalDataSet(dataSet)

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
		if _, err := c.Get(user.ID, retrievedUser); err != nil {
			fmt.Println(c.Name, user.ID)
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

// func TestConcurrentOnOneCollection(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("skipping test in short mode.")
// 	}

// 	testPath := <-getTestPathChan
// 	defer os.RemoveAll(testPath)
// 	db, openDBErr := Open(testPath)
// 	if openDBErr != nil {
// 		t.Error(openDBErr)
// 		return
// 	}
// 	defer db.Close()

// 	c, userDBErr := db.Use("testCol")
// 	if userDBErr != nil {
// 		t.Error(userDBErr)
// 		return
// 	}

// 	if err := setIndexes(c); err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	doneChan := make(chan error, 0)

// 	users1 := unmarshalDataSet(dataSet1)
// 	users2 := unmarshalDataSet(dataSet2)
// 	users3 := unmarshalDataSet(dataSet3)
// 	for i, user := range users1 {
// 		go updateUser(c, user, users2[i], users3[i], doneChan)
// 	}

// 	for i := 0; i < len(users1)-1; i++ {
// 		err := <-doneChan
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}
// 	}

// 	for i := 0; i < 5; i++ {
// 		// for i, _ := range users1 {
// 		// go checkIndexes(c, users1[i], users2[i], users3[i], doneChan)
// 	}

// 	for i := 0; i < len(users1)-1; i++ {
// 		err := <-doneChan
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}
// 	}
// }

func updateUser(c *Collection, v1, v2, v3 *User, done chan error) {
	if err := c.Put(v1.ID, v1); err != nil {
		done <- err
		return
	}
	if err := c.Put(v1.ID, v2); err != nil {
		done <- err
		return
	}
	if err := c.Put(v1.ID, v3); err != nil {
		done <- err
		return
	}

	done <- nil
}
