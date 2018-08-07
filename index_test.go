package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestConcurrentCollections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(ctx, NewDefaultOptions(testPath))
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
			time.Sleep(time.Second * 1)
			return
		}
	}
}

func setIndexes(c *Collection) error {
	indexes := []struct {
		name     string
		selector []string
		t        IndexType
	}{
		{"email", []string{"email"}, StringIndex},
		{"balance", []string{"Balance"}, IntIndex},
		{"city", []string{"Address", "City"}, StringIndex},
		{"zip", []string{"Address", "ZipCode"}, IntIndex},
		{"age", []string{"Age"}, IntIndex},
		{"last login", []string{"LastLogin"}, TimeIndex},
	}

	for _, indexParams := range indexes {
		if err := c.SetIndex(indexParams.name, indexParams.t, indexParams.selector...); err != nil {
			return err
		}
	}
	return nil
}

func insertObjectsForConcurrent(c *Collection, dataSet []byte, done chan error) {
	users := unmarshalDataSet(dataSet)

	for _, user := range users[:1] {
		// for _, user := range users {
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

	for _, user := range users[:1] {
		// for _, user := range users {
		retrievedUser := new(User)
		if _, err := c.Get(user.ID, retrievedUser); err != nil {
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

func updateUser(c *Collection, v1, v2, v3 *User, done chan error) error {
	if err := c.Put(v1.ID, v1); err != nil {
		done <- err
		return err
	}
	if err := c.Put(v1.ID, v2); err != nil {
		done <- err
		return err
	}
	if err := c.Put(v1.ID, v3); err != nil {
		done <- err
		return err
	}

	done <- nil
	return nil
}

func TestGetInfo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, _ := fillUpDB(ctx, t, dataSet1)
	if db == nil {
		return
	}
	defer db.Close()
	defer os.RemoveAll(db.options.Path)

	testPath := db.options.Path

	if err := db.Close(); err != nil {
		t.Error(err)
		return
	}
	db = nil

	var err error
	db, err = Open(ctx, NewDefaultOptions(testPath))
	if err != nil {
		t.Error(err)
		return
	}

	c, userDBErr := db.Use("testCol")
	if userDBErr != nil {
		t.Error(err)
		return
	}

	collections := db.GetCollections()
	if len(collections) != 1 {
		t.Errorf("it must be only one collection and has %d", len(collections))
		return
	}

	errFunc := func(expected, had string) {
		t.Errorf("the index name translation is not correct. Expected %q but had %q", expected, had)
	}

	indexesInfo := c.GetIndexesInfo()
	if len(indexesInfo) != 6 {
		t.Errorf("it must have 6 indexes but has %d", len(indexesInfo))
	}
	for _, indexInfo := range indexesInfo {
		indexTypeAsString := indexInfo.GetType()
		if indexInfo.Type == StringIndex && indexTypeAsString != StringIndexString {
			errFunc(indexTypeAsString, StringIndexString)
		} else if indexInfo.Type == IntIndex && indexTypeAsString != IntIndexString {
			errFunc(indexTypeAsString, IntIndexString)
		} else if indexInfo.Type == TimeIndex && indexTypeAsString != TimeIndexString {
			errFunc(indexTypeAsString, TimeIndexString)
		}
	}

	return
}
