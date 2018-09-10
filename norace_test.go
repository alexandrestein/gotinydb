// +build !race

package gotinydb

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

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

	err = db.Backup(backupArchivePath, 0)
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

	err = restoredDB.Load(backupArchivePath)
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

		q := NewQuery().SetFilter(
			NewEqualFilter(user.Email, "email"),
		).SetLimits(1, 0)

		response, _ = restoredCol.Query(q)
		response.One(gettedUser)

		if !reflect.DeepEqual(user, gettedUser) {
			return fmt.Errorf("user in original database and in restored database are not equal\n\t%v\n\t%v", user, gettedUser)
		}

		q = NewQuery().SetFilter(
			NewEqualFilter(user.Age, "Age"),
		).SetLimits(1, 0)

		gettedUser = new(User)
		response, _ = restoredCol.Query(q)
		response.One(gettedUser)

		if user.Age != gettedUser.Age {
			return fmt.Errorf("query did not returned value with the same age:\n\t%v\n\t%v", user, gettedUser)
		}

		q = NewQuery().SetFilter(
			NewEqualFilter(user.Address.City, "Address", "city"),
		).SetLimits(1, 0)

		gettedUser = new(User)
		response, _ = restoredCol.Query(q)
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
