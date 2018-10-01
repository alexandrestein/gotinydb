package gotinydb

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dgraph-io/badger"
	"golang.org/x/crypto/blake2b"
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

		db.GetCollections()

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

		c.SetIndex("email", StringIndex, "email")

		err = c.Put(testID, testContent)
		if err != nil {
			t.Error(err)
			return
		}

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

			retrievedUser = new(User)
			var response *Response
			response, err = c.Query(c.NewQuery().SetFilter(NewEqualFilter(testContent.Email, "email")))
			if err != nil {
				t.Error(err)
				return
			}

			response.One(retrievedUser)

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

		retrievedUser = new(User)
		var response *Response
		response, err = c.Query(c.NewQuery().SetFilter(NewEqualFilter(testContent.Email, "email")))
		// _, err = c.Get(testID, retrievedUser)
		if err != nil {
			t.Error(err)
			return
		}

		response.One(retrievedUser)

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

	var c *Collection
	c, err = db.Use("testCol")
	if err != nil {
		t.Error(err)
		return
	}

	c.Put(testUser.ID, testUser)

	// To test index option update
	c.SetIndex("test", StringIndex, "nil")

	cryptoKey := [32]byte{}
	rand.Read(cryptoKey[:])

	options := NewDefaultOptions(testPath)
	options.CryptoKey = cryptoKey

	err = db.SetOptions(options)
	if err != nil {
		t.Error(err)
		return
	}
	db.Close()

	// Build a new option because it is degraded after use
	options = NewDefaultOptions(testPath)
	options.CryptoKey = cryptoKey

	db, err = Open(ctx, options)
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	c, err = db.Use("testCol")
	if err != nil {
		t.Error(err)
		return
	}

	retrievedUser := new(User)
	_, err = c.Get(testUser.ID, retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}
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

func TestCollection_PutToCloseDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "putToBadDB"
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

	cancel()

	obj := &struct{ Name string }{"Bad Insertion"}
	err = c.Put("hello", obj)
	if err != ErrClosedDB {
		t.Error("The database must return an error but not the one expected", err)
		return
	}
}

func TestDB_Backup_And_Load(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	backedUpDBPath := os.TempDir() + "/" + "backedUp"
	restoredDBPath := os.TempDir() + "/" + "restored"

	defer os.RemoveAll(backedUpDBPath)
	defer os.RemoveAll(restoredDBPath)

	db, err := Open(ctx, NewDefaultOptions(backedUpDBPath))
	if err != nil {
		t.Error(err)
		return
	}
	defer db.Close()

	names := make([]string, 3)
	baseCols := make([]*Collection, 3)
	for i, n := range []int{1, 2, 3} {
		names[i] = fmt.Sprintf("collection test %d", n)

		var tmpC *Collection
		tmpC, err = db.Use(names[i])
		if err != nil {
			t.Error(err)
			return
		}

		baseCols[i] = tmpC
	}

	addIndexesFunc := func(c *Collection) {
		c.SetIndex("email", StringIndex, "email")
		c.SetIndex("age", UIntIndex, "Age")
		c.SetIndex("city", StringIndex, "Address", "city")

		c.SetBleveIndex("index 2", bleve.NewIndexMapping())
	}
	addIndexesFunc(baseCols[0])
	addIndexesFunc(baseCols[1])
	addIndexesFunc(baseCols[2])

	addContentFunc := func(c *Collection, ds dataset) {
		for _, user := range unmarshalDataset(ds) {
			err = c.Put(user.ID, user)
			if err != nil {
				t.Error(err.Error())
				return
			}
		}
	}
	addContentFunc(baseCols[0], dataset1)
	addContentFunc(baseCols[1], dataset2)
	addContentFunc(baseCols[2], dataset3)

	var backup bytes.Buffer

	_, err = db.Backup(&backup, 0)
	if err != nil {
		t.Error(err)
		return
	}

	var restoredDB *DB
	restoredDB, err = Open(ctx, NewDefaultOptions(restoredDBPath))
	if err != nil {
		t.Error(err)
		return
	}

	err = restoredDB.Load(&backup)
	if err != nil {
		t.Error(err)
		return
	}

	restoredCols := make([]*Collection, 3)
	for i := range []int{1, 2, 3} {
		var tmpC *Collection
		tmpC, err = restoredDB.Use(names[i])
		if err != nil {
			t.Error(err)
			return
		}

		restoredCols[i] = tmpC
	}

	var ids []string
	for _, user := range unmarshalDataset(dataset1) {
		ids = append(ids, user.ID)
	}

	// Test simple get values
	err = backupAndRestorSimpleGetValues(ids, baseCols[0], baseCols[1], baseCols[2], restoredCols[0], restoredCols[1], restoredCols[2])
	if err != nil {
		t.Error(err)
		return
	}

	err = backupAndRestorQueries(ids, baseCols[0], baseCols[1], baseCols[2], restoredCols[0], restoredCols[1], restoredCols[2])
	if err != nil {
		t.Error(err)
		return
	}
}

func backupAndRestorSimpleGetValues(ids []string, c1, c2, c3, rc1, rc2, rc3 *Collection) (err error) {
	var values []*ResponseElem

	testValues := func(values []*ResponseElem, rc *Collection) error {
		for _, response := range values {
			user := &User{}
			restoredUser := &User{}
			err = response.Unmarshal(user)
			if err != nil {
				return err
			}

			_, err = rc.Get(user.ID, restoredUser)
			if err != nil {
				return err
			}

			if !reflect.DeepEqual(user, restoredUser) {
				return fmt.Errorf("restored element and saved element are not equal: \n\t%v\n\t%v", user, restoredUser)
			}
		}
		return nil
	}

	values, err = c1.GetValues(ids[0], len(ids))
	if err != nil {
		return err
	}
	err = testValues(values, rc1)
	if err != nil {
		return err
	}

	values, err = c2.GetValues(ids[0], len(ids))
	if err != nil {
		return err
	}
	err = testValues(values, rc2)
	if err != nil {
		return err
	}

	values, err = c3.GetValues(ids[0], len(ids))
	if err != nil {
		return err
	}
	err = testValues(values, rc3)
	if err != nil {
		return err
	}

	return nil
}

func backupAndRestorQueries(ids []string, c1, c2, c3, rc1, rc2, rc3 *Collection) (err error) {
	user := &User{}
	gettedUser := &User{}
	var response *Response

	testFunc := func(id string, baseCol, restoredCol *Collection) (err error) {
		baseCol.Get(id, user)

		q := restoredCol.NewQuery().SetFilter(
			NewEqualFilter(user.Email, "email"),
		).SetLimits(1, 0)

		response, err = restoredCol.Query(q)
		if err != nil {
			return err
		}
		response.One(gettedUser)

		if !reflect.DeepEqual(user, gettedUser) {
			return fmt.Errorf("user in original database and in restored database are not equal\n\t%v\n\t%v", user, gettedUser)
		}

		q = restoredCol.NewQuery().SetFilter(
			NewEqualFilter(user.Age, "Age"),
		).SetLimits(1, 0)

		gettedUser = new(User)
		response, err = restoredCol.Query(q)
		if err != nil {
			return err
		}
		response.One(gettedUser)

		if user.Age != gettedUser.Age {
			return fmt.Errorf("query did not returned value with the same age:\n\t%v\n\t%v", user, gettedUser)
		}

		q = restoredCol.NewQuery().SetFilter(
			NewEqualFilter(user.Address.City, "Address", "city"),
		).SetLimits(1, 0)

		gettedUser = new(User)
		response, err = restoredCol.Query(q)
		if err != nil {
			return err
		}
		response.One(gettedUser)

		if user.Address.City != gettedUser.Address.City {
			return fmt.Errorf("query did not returned value with the same city:\n\t%v\n\t%v", user, gettedUser)
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

func TestFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := os.TempDir() + "/" + "file"
	defer os.RemoveAll(testPath)

	opt := NewDefaultOptions(testPath)
	// 235KB
	opt.FileChunkSize = 235 * 100

	db, err := Open(ctx, opt)
	if err != nil {
		t.Error(err)
		return
	}

	// 100MB, it will be made 4256 chunks
	randBuff := make([]byte, 100*1000*1000)
	rand.Read(randBuff)

	fileID := "test file ID"

	err = db.PutFile(fileID, bytes.NewBuffer(randBuff))
	if err != nil {
		t.Error(err)
		return
	}

	randHash := blake2b.Sum256(randBuff)

	readBuff := bytes.NewBuffer(nil)
	err = db.ReadFile(fileID, readBuff)
	if err != nil {
		t.Error(err)
		return
	}

	readHash := blake2b.Sum256(readBuff.Bytes())

	if !reflect.DeepEqual(randHash, readHash) {
		t.Error("the saved file and the rand file are not equal")
		return
	}

	// Check the ids with chunk number are well generated
	err = db.badgerDB.View(func(txn *badger.Txn) error {
		storeID := db.buildFilePrefix(fileID, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		it := txn.NewIterator(opt)
		defer it.Close()
		prevLastByte := -1
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			lastByte := int(it.Item().Key()[len(it.Item().Key())-1:][0])
			if prevLastByte+1 != lastByte {
				if prevLastByte == 255 && lastByte != 0 {
					t.Errorf("generated incremental bytes is not good")
				}
			}
			prevLastByte = lastByte
		}

		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}

	err = db.DeleteFile(fileID)
	if err != nil {
		t.Error(err)
		return
	}

	err = db.badgerDB.View(func(txn *badger.Txn) error {
		storeID := db.buildFilePrefix(fileID, -1)

		opt := badger.DefaultIteratorOptions
		opt.PrefetchValues = false

		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Seek(storeID); it.ValidForPrefix(storeID); it.Next() {
			return fmt.Errorf("must be empty response")
		}

		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
}
