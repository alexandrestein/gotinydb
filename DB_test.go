package gotinydb

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	t.Run("Simple Opening", func(t *testing.T) {
		testPath := os.TempDir() + "/" + "openingTest"
		defer os.RemoveAll(testPath)

		db, err := Open(ctx, NewDefaultOptions(testPath))
		if err != nil {
			t.Error(err)
			return
		}

		err = db.Close()
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Opening Wrong Path", func(t *testing.T) {
		_, err := Open(ctx, NewDefaultOptions("/path/does/not/exist"))
		if err == nil {
			t.Errorf("database opened but path does not exist")
		}
	})

	t.Run("Opening With No Badger Config", func(t *testing.T) {
		testPath := os.TempDir() + "/" + "openingNoBadgerConfigTest"
		defer os.RemoveAll(testPath)

		options := NewDefaultOptions(testPath)
		options.BadgerOptions = nil
		_, err := Open(ctx, options)
		if err == nil {
			t.Errorf("database opened but Badger config is not correct")
		}
	})
}

func TestDB_Use(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "use"
	defer os.RemoveAll(testPath)

	colName := "testCol"
	testID := "testID"
	testContent := &User{
		ID:        testID,
		Age:       10,
		Email:     "test@test.com",
		Balance:   2186,
		LastLogin: time.Now().Truncate(time.Millisecond),
		Address: &Address{
			City:    "San Francisco",
			ZipCode: 94102,
		},
	}

	var c *Collection

	t.Run("Initial Use Call", func(t *testing.T) {
		db, err := Open(ctx, NewDefaultOptions(testPath))
		if err != nil {
			t.Error(err)
			return
		}
		defer db.Close()

		c, err = db.Use(colName)
		if err != nil {
			t.Error(err)
			return
		}

		c.Put(testID, testContent)

		t.Run("Ask Twice the same collection", func(t *testing.T) {
			c, err = db.Use(colName)
			if err != nil {
				t.Error(err)
				return
			}

			retrievedUser := new(User)
			_, err = c.Get(testID, retrievedUser)
			if err != nil {
				t.Error(err)
				return
			}

			if !reflect.DeepEqual(testContent, retrievedUser) {
				t.Errorf("both users are not equal but should\n\t%v\n\t%v", testContent, retrievedUser)
				return
			}
		})
	})

	t.Run("Second Use Call", func(t *testing.T) {
		db, err := Open(ctx, NewDefaultOptions(testPath))
		if err != nil {
			t.Error(err)
			return
		}
		defer db.Close()

		c, err = db.Use(colName)
		if err != nil {
			t.Error(err)
			return
		}

		retrievedUser := new(User)
		_, err = c.Get(testID, retrievedUser)
		if err != nil {
			t.Error(err)
			return
		}

		if !reflect.DeepEqual(testContent, retrievedUser) {
			t.Errorf("both users are not equal but should\n\t%v\n\t%v", testContent, retrievedUser)
			return
		}
	})
}

func TestDB_SetOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "setOptions"
	defer os.RemoveAll(testPath)

	db, err := Open(ctx, NewDefaultOptions(testPath))
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	_, err = db.Use("testCol")
	if err != nil {
		t.Error(err)
		return
	}

	db.SetOptions(NewDefaultOptions(testPath))
}

func TestDB_DeleteCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "deleteCollections"
	defer os.RemoveAll(testPath)

	t.Run("Delete Empty Collections", func(t *testing.T) {
		db, err := Open(ctx, NewDefaultOptions(testPath))
		if err != nil {
			t.Error(err)
			return
		}
		defer db.Close()

		name1 := "collection test 1"
		name2 := "collection test 2"

		_, err = db.Use(name1)
		if err != nil {
			t.Error(err)
			return
		}
		_, err = db.Use(name2)
		if err != nil {
			t.Error(err)
			return
		}

		err = db.DeleteCollection(name1)
		if err != nil {
			t.Errorf("collection should be removed without issue")
			return
		}
		err = db.DeleteCollection(name2)
		if err != nil {
			t.Errorf("collection should be removed without issue")
			return
		}
	})

	t.Run("Delete Empty With Indexes Collections", func(t *testing.T) {
		db, err := Open(ctx, NewDefaultOptions(testPath))
		if err != nil {
			t.Error(err)
			return
		}
		defer db.Close()

		name := "collection with indexes"

		var c *Collection
		c, err = db.Use(name)
		if err != nil {
			t.Error(err)
			return
		}

		c.SetIndex("email", StringIndex, "email")
		c.SetIndex("age", IntIndex, "Age")

		err = db.DeleteCollection(name)
		if err != nil {
			t.Errorf("collection should be removed without issue")
			return
		}
	})

	t.Run("Delete Indexes Collections With Values", func(t *testing.T) {
		db, err := Open(ctx, NewDefaultOptions(testPath))
		if err != nil {
			t.Error(err)
			return
		}
		defer db.Close()

		name := "collection with indexes and values"

		var c *Collection
		c, err = db.Use(name)
		if err != nil {
			t.Error(err)
			return
		}

		c.SetIndex("email", StringIndex, "email")
		c.SetIndex("age", IntIndex, "Age")

		for _, user := range unmarshalDataset(dataset1) {
			err = c.Put(user.ID, user)
			if err != nil {
				t.Error(err)
				return
			}
		}

		err = db.DeleteCollection(name)
		if err != nil {
			t.Errorf("collection should be removed without issue but: %s", err.Error())
			return
		}
	})
}

func TestDB_Backup_And_Load(t *testing.T) {
	t0 := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	backedUpDBPath := os.TempDir() + "/" + "backedUp"
	backupArchivePath := os.TempDir() + "/" + "bkpArchive"
	restoredDBPath := os.TempDir() + "/" + "restored"

	defer os.RemoveAll(backedUpDBPath)
	defer os.RemoveAll(backupArchivePath)
	defer os.RemoveAll(restoredDBPath)

	db, err := Open(ctx, NewDefaultOptions(backedUpDBPath))
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	name1 := "collection test 1"
	name2 := "collection test 2"
	name3 := "collection test 3"

	var c1, c2, c3 *Collection

	c1, err = db.Use(name1)
	if err != nil {
		t.Error(err)
		return
	}
	c2, err = db.Use(name2)
	if err != nil {
		t.Error(err)
		return
	}
	c3, err = db.Use(name3)
	if err != nil {
		t.Error(err)
		return
	}

	addIndexesFunc := func(c *Collection) {
		c.SetIndex("email", StringIndex, "email")
		c.SetIndex("age", IntIndex, "Age")
	}
	addIndexesFunc(c1)
	addIndexesFunc(c2)
	addIndexesFunc(c3)

	addContentFunc := func(c *Collection, ds dataset) {
		for _, user := range unmarshalDataset(ds) {
			err = c.Put(user.ID, user)
			if err != nil {
				t.Error(err.Error())
				return
			}
		}
	}
	addContentFunc(c1, dataset1)
	addContentFunc(c2, dataset2)
	addContentFunc(c3, dataset3)

	if testing.Verbose() {
		t.Logf("Initial database fill up done %s after the begin of the test", time.Since(t0).String())
	}

	err = db.Backup(backupArchivePath, 0)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Logf("Backup done %s after the begin of the test", time.Since(t0).String())
	}

	var restoredDB *DB
	restoredDB, err = Open(ctx, NewDefaultOptions(restoredDBPath))
	if err != nil {
		t.Error(err)
		return
	}

	err = restoredDB.Load(backupArchivePath)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Logf("Loading done %s after the begin of the test", time.Since(t0).String())
	}

	var rc1, rc2, rc3 *Collection
	rc1, err = restoredDB.Use(name1)
	if err != nil {
		t.Error(err)
		return
	}
	rc2, err = restoredDB.Use(name2)
	if err != nil {
		t.Error(err)
		return
	}
	rc3, err = restoredDB.Use(name3)
	if err != nil {
		t.Error(err)
		return
	}

	var ids []string
	for _, user := range unmarshalDataset(dataset1) {
		ids = append(ids, user.ID)
	}

	if testing.Verbose() {
		t.Logf("Start checking restoration %s after the begin of the test", time.Since(t0).String())
	}

	// Test simple get values
	err = backupAndRestorSimpleGetValues(ids, c1, c2, c3, rc1, rc2, rc3)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Logf("Get check is done %s after the begin of the test", time.Since(t0).String())
		t.Logf("Starting check by querying the indexes")
	}

	err = backupAndRestorQueries(ids, c1, c2, c3, rc1, rc2, rc3)
	if err != nil {
		t.Error(err)
		return
	}
}

func backupAndRestorSimpleGetValues(ids []string, c1, c2, c3, rc1, rc2, rc3 *Collection) (err error) {
	var values []*ResponseElem
	values, err = c1.GetValues(ids[0], len(ids))
	if err != nil {
		return err
	}
	for _, response := range values {
		user := &User{}
		restoredUser := &User{}
		err = response.Unmarshal(user)
		if err != nil {
			return err
		}

		_, err = rc1.Get(user.ID, restoredUser)
		if err != nil {
			return err
		}

		if !reflect.DeepEqual(user, restoredUser) {
			return fmt.Errorf("restored element and saved element are not equal: \n\t%v\n\t%v", user, restoredUser)
		}
	}

	values, err = c2.GetValues(ids[0], len(ids))
	if err != nil {
		return err
	}
	for _, response := range values {
		user := &User{}
		restoredUser := &User{}
		err = response.Unmarshal(user)
		if err != nil {
			return err
		}

		_, err = rc2.Get(user.ID, restoredUser)
		if err != nil {
			return err
		}

		if !reflect.DeepEqual(user, restoredUser) {
			return fmt.Errorf("restored element and saved element are not equal: \n\t%v\n\t%v", user, restoredUser)
		}
	}

	values, err = c3.GetValues(ids[0], len(ids))
	if err != nil {
		return err
	}
	for _, response := range values {
		user := &User{}
		restoredUser := &User{}
		err = response.Unmarshal(user)
		if err != nil {
			return err
		}

		_, err = rc3.Get(user.ID, restoredUser)
		if err != nil {
			return err
		}

		if !reflect.DeepEqual(user, restoredUser) {
			return fmt.Errorf("restored element and saved element are not equal: \n\t%v\n\t%v", user, restoredUser)
		}
	}

	return nil
}

func backupAndRestorQueries(ids []string, c1, c2, c3, rc1, rc2, rc3 *Collection) (err error) {
	user := &User{}
	gettedUser := &User{}
	var response *Response

	testFunc := func(id string, baseCol, restoredCol *Collection) (err error) {
		baseCol.Get(id, user)

		q := NewQuery().SetFilter(
			NewFilter(Equal).SetSelector("email").CompareTo(user.Email),
		).SetLimits(1, 1)

		response, err = restoredCol.Query(q)
		response.One(gettedUser)

		if !reflect.DeepEqual(user, gettedUser) {
			return fmt.Errorf("user in original database and in restored database are not equal\n\t%v\n\t%v", user, gettedUser)
		}

		baseCol.Get(id, user)

		q = NewQuery().SetFilter(
			NewFilter(Equal).SetSelector("Age").CompareTo(user.Age),
		).SetLimits(1, 1)

		response, err = restoredCol.Query(q)
		response.One(gettedUser)

		if !reflect.DeepEqual(user, gettedUser) {
			return fmt.Errorf("user in original database and in restored database are not equal\n\t%v\n\t%v", user, gettedUser)
		}

		return nil
	}

	for _, id := range ids {
		err = testFunc(id, c1, rc1)
		if err != nil {
			return err
		}
		err = testFunc(id, c2, rc2)
		if err != nil {
			return err
		}
		err = testFunc(id, c3, rc3)
		if err != nil {
			return err
		}
	}

	return nil
}
