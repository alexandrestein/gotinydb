package gotinydb

// import (
// 	"context"
// 	"crypto/rand"
// 	"os"
// 	"reflect"
// 	"sync"
// 	"testing"
// 	"time"
// )

// func TestCollection_PutGetAndDelete(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "putGetAndDelete"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	err = c.SetIndex("email", StringIndex, "email")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	err = c.SetIndex("email", StringIndex, "email")
// 	if err == nil {
// 		t.Errorf("must return error")
// 		return
// 	}

// 	u := testUser

// 	err = c.Put(u.ID, u)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	retrievedUser := new(User)
// 	_, err = c.Get(u.ID, retrievedUser)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	if !reflect.DeepEqual(u, retrievedUser) {
// 		t.Errorf("both users are not equal but should\n\t%v\n\t%v", u, retrievedUser)
// 		return
// 	}

// 	err = c.Delete(u.ID)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	_, err = c.Get(u.ID, nil)
// 	if err == nil {
// 		t.Errorf("element has been removed and didn't get any error")
// 		return
// 	}

// 	_, err = c.Query(
// 		c.NewQuery().SetFilter(
// 			NewEqualFilter("clement-38@thurmond.com", "email"),
// 		),
// 	)
// 	if err != ErrNotFound {
// 		t.Errorf("this must return an not found error")
// 		return
// 	}

// 	err = db.Close()
// 	if err != nil {
// 		t.Error(err)
// 	}
// }

// func TestCollection_PutGetAndDeleteBin(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "binTests"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("bins collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	err = c.SetIndex("email", StringIndex, "email")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	contentID := "id"
// 	content := make([]byte, 1024)
// 	rand.Read(content)

// 	err = c.Put(contentID, content)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	retBytes, _ := c.Get(contentID, nil)

// 	if !reflect.DeepEqual(retBytes, content) {
// 		t.Errorf("the bin content are not equal")
// 		return
// 	}

// 	// Test to update with a index element
// 	c.Put(contentID, testUser)
// 	userFromQuery := new(User)
// 	response, _ := c.Query(c.NewQuery().SetFilter(
// 		NewEqualFilter(testUser.Email, "email"),
// 	))
// 	response.One(userFromQuery)
// 	if !reflect.DeepEqual(userFromQuery, testUser) {
// 		t.Errorf("save user is not equal")
// 		return
// 	}

// 	// Add an an other bin
// 	rand.Read(content)
// 	err = c.Put(contentID, content)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	_, err = c.Query(c.NewQuery().SetFilter(
// 		NewEqualFilter(testUser.Email, "email"),
// 	))
// 	if err != ErrNotFound {
// 		t.Errorf("the element is not present any more")
// 		return
// 	}

// 	retBytes, _ = c.Get(contentID, nil)
// 	if !reflect.DeepEqual(retBytes, content) {
// 		t.Errorf("the bin content are not equal")
// 		return
// 	}
// }

// func TestCollection_PutMulti(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "putMulti"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	err = c.SetIndex("email", StringIndex, "email")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	var user120 *User

// 	var IDs []string
// 	var content []interface{}
// 	for _, user := range unmarshalDataset(dataset1) {
// 		IDs = append(IDs, user.ID)
// 		content = append(content, user)

// 		if user.ID == "120" {
// 			user120 = user
// 		}
// 	}

// 	err = c.PutMulti(IDs, content)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	retrievedUser := &User{}
// 	_, err = c.Get("120", retrievedUser)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	if !reflect.DeepEqual(user120, retrievedUser) {
// 		t.Errorf("both users are not equal but should\n\t%v\n\t%v", user120, retrievedUser)
// 		return
// 	}

// 	err = db.Close()
// 	if err != nil {
// 		t.Error(err)
// 	}
// }

// func TestCollection_MultiPutAndDelete(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "putMulti"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	// Tries to run delete and put in the same time to test mixed write request
// 	var wg sync.WaitGroup
// 	wg.Add(3)
// 	go func() {
// 		c.Put("id", []byte("empty"))
// 		wg.Done()
// 	}()
// 	go func() {
// 		c.Delete("id")
// 		wg.Done()
// 	}()
// 	go func() {
// 		c.Put("id", []byte("empty"))
// 		wg.Done()
// 	}()

// 	wg.Wait()
// }

// func TestCollection_DeleteIndex(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "deleteIndex"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	indexName := "email"
// 	err = c.SetIndex(indexName, StringIndex, "email")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	var IDs []string
// 	var content []interface{}
// 	for _, user := range unmarshalDataset(dataset1) {
// 		IDs = append(IDs, user.ID)
// 		content = append(content, user)
// 	}

// 	err = c.PutMulti(IDs, content)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	err = c.DeleteIndex(indexName)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	err = c.DeleteIndex(indexName)
// 	if err == nil {
// 		t.Error("the index does not exist and this must return an error")
// 		return
// 	}
// }

// func TestCollection_GetIDsAndValues(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "getIDsAndValues"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	users := unmarshalDataset(dataset1)

// 	var IDs []string
// 	var content []interface{}

// 	for _, user := range users {
// 		IDs = append(IDs, user.ID)
// 		content = append(content, user)
// 	}

// 	err = c.PutMulti(IDs, content)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	var ids []string
// 	ids, _ = c.GetIDs("", len(users))

// 	var values []*ResponseElem
// 	values, _ = c.GetValues("", len(users))

// 	if len(users) != len(ids) || len(users) != len(values) {
// 		t.Errorf("the length of the returned elements are not what is expected\n\tnumbers of users: %d\n\tnumbers of ids: %d\n\tnumbers of values: %d", len(users), len(ids), len(values))
// 		return
// 	}

// 	for i := range ids {
// 		userFromValues := &User{}
// 		err = values[i].Unmarshal(userFromValues)
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		if ids[i] != values[i].GetID() {
// 			t.Errorf("the IDs are not equal: %q and %q", ids[i], values[i].GetID())
// 			return
// 		}
// 	}
// }

// func TestCollection_Rollback_And_Concurrent_Writes(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "rollback"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	users := unmarshalDataset(dataset1)
// 	users2 := unmarshalDataset(dataset2)
// 	users3 := unmarshalDataset(dataset3)

// 	var wg sync.WaitGroup
// 	wg.Add(len(users))
// 	for i, user := range users {
// 		go func(c *Collection, id string, u1, u2, u3 *User) {
// 			c.Put(id, u1)
// 			c.Put(id, u2)
// 			c.Put(id, u3)
// 			wg.Done()
// 		}(c, user.ID, user, users2[i], users3[i])
// 	}
// 	wg.Wait()

// 	for i := 0; i < len(users); i++ {
// 		if i%2 == 0 {
// 			_, err = c.Rollback(users[i].ID, 0)
// 			if err != nil {
// 				t.Error(err)
// 				return
// 			}
// 			retrievedUser := &User{}
// 			_, err = c.Get(users[i].ID, retrievedUser)
// 			if err != nil {
// 				t.Error(err)
// 				return
// 			}

// 			if !reflect.DeepEqual(users2[i], retrievedUser) {
// 				t.Errorf("both users are not equal but should\n\t%v\n\t%v", users2[i], retrievedUser)
// 				return
// 			}
// 		} else {
// 			_, err = c.Rollback(users[i].ID, 1)
// 			if err != nil {
// 				t.Error(err)
// 				return
// 			}
// 			retrievedUser := &User{}
// 			_, err = c.Get(users[i].ID, retrievedUser)
// 			if err != nil {
// 				t.Error(err)
// 				return
// 			}

// 			if !reflect.DeepEqual(users[i], retrievedUser) {
// 				t.Errorf("both users are not equal but should\n\t%v\n\t%v", users[i], retrievedUser)
// 				return
// 			}
// 		}
// 	}

// 	_, err = c.Rollback(users[0].ID, 10)
// 	if err == nil {
// 		t.Errorf("no error was returned but the function should return an error")
// 		return
// 	}
// 	if err != ErrRollbackVersionNotFound {
// 		t.Errorf("the returned error is not what is expected. Expect %q but had %q", ErrRollbackVersionNotFound.Error(), err.Error())
// 		return
// 	}
// }

// func TestCollection_GetIndexesInfo(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := os.TempDir() + "/" + "getIndexInfo"
// 	defer os.RemoveAll(testPath)

// 	db, err := Open(ctx, NewDefaultOptions(testPath))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c.SetIndex("email", StringIndex, "email")
// 	c.SetIndex("age", IntIndex, "Age")
// 	c.SetIndex("last connection", TimeIndex, "history", "lastConnection")

// 	expectedIndexInfos := []*IndexInfo{
// 		{Name: "email", Selector: []string{"email"}, Type: StringIndex},
// 		{Name: "age", Selector: []string{"Age"}, Type: IntIndex},
// 		{Name: "last connection", Selector: []string{"history", "lastConnection"}, Type: TimeIndex},
// 	}

// 	infos := c.GetIndexesInfo()
// 	for i, info := range infos {
// 		if !reflect.DeepEqual(info, expectedIndexInfos[i]) {
// 			t.Errorf("returned index info is not what is expected\n\t%v\n\t%v", info, expectedIndexInfos[i])
// 			return
// 		}
// 	}
// }
