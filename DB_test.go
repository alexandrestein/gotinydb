package gotinydb

import (
	"context"
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
		_, err := Open(ctx, NewDefaultOptions(os.DevNull))
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
	testContent := testUser

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
