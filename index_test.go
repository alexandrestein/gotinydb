package gotinydb

import (
	cryptoRand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/alexandrestein/gotinydb/vars"
)

func init() {
	buf := make([]byte, 8)
	cryptoRand.Read(buf)
	// buff := bytes.NewBuffer(buf)
	intVal := binary.LittleEndian.Uint64(buf)
	rand.Seed(int64(intVal))
}

func TestStringIndex(t *testing.T) {
	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()
	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	// Build the index
	index := new(Index)
	index.Name = "test index"
	index.Selector = []string{"Login"}
	index.Type = vars.StringIndex
	c.SetIndex(index)

	// Build a list of "user"
	list := buildRandLogins(2000)
	// Loop on users to insert it into the database
	for i, name := range list {
		id := vars.BuildID(name)
		user := struct{ Login, Pass string }{name, vars.BuildID(name + name)}
		if err := c.Put(id, user); err != nil {
			t.Error(err)
			return
		}

		// Add some duplicated field to have multiple IDs for one field value
		if i%3 == 0 {
			id := vars.BuildID(name + "_bis")
			user := struct{ Login, Pass string }{name, vars.BuildID(name + name + name)}
			if err := c.Put(id, user); err != nil {
				t.Error(err)
				return
			}
		}
	}

	// Query Login equal
	for i := 2000; i > 0; i-- {
		randInt := rand.Intn(2000)
		getAction := NewAction(Equal).CompareTo(list[randInt]).SetSelector([]string{"Login"})
		queryObj := NewQuery().SetLimit(1).Get(getAction)

		queryResponse, err := c.Query(queryObj)
		if err != nil {
			t.Error(err)
			return
		}
		if queryResponse.Len() != 1 {
			t.Errorf("response returned other there one ID: %v", queryResponse)
			return
		}
	}
}

func TestStringIndexRange(t *testing.T) {
	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()
	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	// Build the index
	index := new(Index)
	index.Name = "test index"
	index.Selector = []string{"Login"}
	index.Type = vars.StringIndex
	c.SetIndex(index)

	// Build a list of "user"
	list := buildRandLogins(2000)
	// Loop on users to insert it into the database
	for i, name := range list {
		id := vars.BuildID(name)
		user := struct{ Login, Pass string }{name, vars.BuildID(name + name)}
		if err := c.Put(id, user); err != nil {
			t.Error(err)
			return
		}

		// Add some duplicated field to have multiple IDs for one field value
		if i%3 == 0 {
			id := vars.BuildID(name + "_bis")
			user := struct{ Login, Pass string }{name, vars.BuildID(name + name + name)}
			if err := c.Put(id, user); err != nil {
				t.Error(err)
				return
			}
		}
	}

	// Query Login greater
	for i := 20; i > 0; i-- {
		randInt := rand.Intn(2000)
		getAction := NewAction(Greater).CompareTo(list[randInt]).SetSelector([]string{"Login"}).EqualWanted()
		queryObj := NewQuery().SetLimit(10).Get(getAction)

		ids, err := c.Query(queryObj)
		if err != nil {
			t.Error(err)
			return
		}

		ids.Range(func(id string, _ []byte) error {
			user := struct{ Login, Pass string }{}
			getErr := c.Get(id, &user)
			if getErr != nil {
				t.Error(getErr.Error())
				return getErr
			}
			if strings.ToLower(list[randInt]) > strings.ToLower(user.Login) {
				err := fmt.Errorf("returned value %q is smaller than comparator %q", user.Login, list[randInt])
				t.Error(err.Error())
				return err
			}
			return nil
		})

		// for _, id := range ids. {
		// 	user := struct{ Login, Pass string }{}
		// 	getErr := c.Get(id, &user)
		// 	if getErr != nil {
		// 		t.Error(getErr.Error())
		// 		return
		// 	}
		// 	if strings.ToLower(list[randInt]) > strings.ToLower(user.Login) {
		// 		t.Errorf("returned value %q is smaller than comparator %q", user.Login, list[randInt])
		// 		return
		// 	}
		// }
	}

	// Query Login less
	for i := 20; i > 0; i-- {
		randInt := rand.Intn(2000)
		getAction := NewAction(Less).CompareTo(list[randInt]).SetSelector([]string{"Login"})
		queryObj := NewQuery().SetLimit(10).Get(getAction)

		ids, err := c.Query(queryObj)
		if err != nil {
			t.Error(err)
			return
		}

		ids.Range(func(id string, _ []byte) error {
			user := struct{ Login, Pass string }{}
			getErr := c.Get(id, &user)
			if getErr != nil {
				t.Error(getErr.Error())
				return getErr
			}
			if strings.ToLower(list[randInt]) < strings.ToLower(user.Login) {
				err := fmt.Errorf("returned value %q is greater than comparator %q", user.Login, list[randInt])
				t.Error(err)
				return err
			}
			return nil
		})
	}
}

func TestStringIndexRangeClean(t *testing.T) {
	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()
	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	// Build the index
	index := new(Index)
	index.Name = "test index"
	index.Selector = []string{"Login"}
	index.Type = vars.StringIndex
	c.SetIndex(index)

	// Build a list of "user"
	list := buildRandLogins(2000)
	// Loop on users to insert it into the database
	for i, name := range list {
		id := vars.BuildID(name)
		user := struct{ Login, Pass string }{name, vars.BuildID(name + name)}
		if err := c.Put(id, user); err != nil {
			t.Error(err)
			return
		}

		// Add some duplicated field to have multiple IDs for one field value
		if i%3 == 0 {
			id := vars.BuildID(name + "_bis")
			user := struct{ Login, Pass string }{name, vars.BuildID(name + name + name)}
			if err := c.Put(id, user); err != nil {
				t.Error(err)
				return
			}
		}
	}

	// Query Login greater
	for i := 20; i > 0; i-- {
		randInt := rand.Intn(2000)
		getAction := NewAction(Greater).CompareTo(list[randInt]).SetSelector([]string{"Login"}).EqualWanted()
		cleanAction := NewAction(Equal).CompareTo(list[randInt]).SetSelector([]string{"Login"}).EqualWanted()
		queryObj := NewQuery().SetLimit(10).Get(getAction).Clean(cleanAction)

		ids, err := c.Query(queryObj)
		if err != nil {
			t.Error(err)
			return
		}

		ids.Range(func(id string, _ []byte) error {
			user := struct{ Login, Pass string }{}
			getErr := c.Get(id, &user)
			if getErr != nil {
				t.Error(getErr.Error())
				return getErr
			}
			if strings.ToLower(list[randInt]) > strings.ToLower(user.Login) {
				err := fmt.Errorf("returned value %q is smaller than comparator %q", user.Login, list[randInt])
				t.Error(err.Error())
				return err
			}
			return nil
		})
	}
}

func TestStringIndexMultipleRange(t *testing.T) {
	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()
	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	// Build the index
	index := new(Index)
	index.Name = "test index"
	index.Selector = []string{"Login"}
	index.Type = vars.StringIndex
	c.SetIndex(index)

	for _, name := range names {
		id := vars.BuildID(name)
		user := struct{ Login, Pass string }{name, vars.BuildID(name + name)}
		if err := c.Put(id, user); err != nil {
			t.Error(err)
			return
		}
	}

	getAction1 := NewAction(Greater).
		CompareTo("Domingo").
		SetSelector([]string{"Login"}).
		EqualWanted().
		SetLimit(5)
	getAction2 := NewAction(Greater).
		CompareTo("Rob").
		SetSelector([]string{"Login"}).
		SetLimit(5)

	cleanAction1 := NewAction(Equal).
		CompareTo("Donald").
		SetSelector([]string{"Login"})
	cleanAction2 := NewAction(Equal).
		CompareTo("Robbins").
		SetSelector([]string{"Login"})

	queryObj := NewQuery().SetLimit(100).
		Get(getAction1).Get(getAction2).
		Clean(cleanAction1).Clean(cleanAction2)

	ids, err := c.Query(queryObj)
	if err != nil {
		t.Error(err)
		return
	}

	expectedValues := [][]string{
		{"Donahue", "AhSq2oDvSgGpfXsDWUxFww"},
		{"Donaldson", "ROhqOcUK078Zsd7ryGd4jw"},
		{"Robbin", "iIc0zgHdf1ArhvYifdFf4A"},
		{"Domingo", "XylxIbLUb9YU6sOJe6-eFQ"},
		{"Robbie", "ejBNV2UKJ7zgQprhNPZp8A"},
		{"Roberson", "Xs3m2cgxN8ZRj0RZRrbicw"},
		{"Robby", "4N64M99oJnCWA-_LO2Cn5w"},
		{"Dominguez", "ggAHrv_BNodNvrUMBuKvSw"},
	}

	ids.Range(func(id string, objectsAsBytes []byte) error {
		user := struct{ Login, Pass string }{}
		getErr := c.Get(id, &user)
		if getErr != nil {
			t.Error(getErr.Error())
			return getErr
		}

		if parseErr := json.Unmarshal(objectsAsBytes, &user); parseErr != nil {
			t.Error(parseErr)
			return parseErr
		}

		for _, expectedValue := range expectedValues {
			if expectedValue[0] == user.Login && expectedValue[1] == user.Pass {
				return nil
			}
		}

		err := fmt.Errorf("Expected value not found but had %v", user)
		t.Error(err.Error())
		return err
	})

	// for _, id := range ids {

	// 	// if strings.ToLower(list[randInt]) > strings.ToLower(user.Login) {
	// 	// 	t.Errorf("returned value %q is smaller than comparator %q", user.Login, list[randInt])
	// 	// 	return
	// 	// }
	// }
}

func buildRandLogins(n int) (ret []string) {
	for i := 0; i < n; i++ {
		randInt := rand.Intn(len(names))
		ret = append(ret, names[randInt])
	}
	return
}

func TestStringIndexDelete(t *testing.T) {
	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBerr := Open(testPath)
	if openDBerr != nil {
		t.Error(openDBerr)
		return
	}
	defer db.Close()
	c, userErr := db.Use("testCol")
	if userErr != nil {
		t.Error(userErr)
		return
	}

	// Build the index
	index := new(Index)
	index.Name = "test index"
	index.Selector = []string{"Login"}
	index.Type = vars.StringIndex
	c.SetIndex(index)

	// Build a list of "user"
	list := buildRandLogins(2000)
	// Loop on users to insert it into the database
	for i, name := range list {
		id := vars.BuildID(name)
		user := struct{ Login, Pass string }{name, vars.BuildID(name + name)}
		if err := c.Put(id, user); err != nil {
			t.Error(err)
			return
		}

		// Add some duplicated field to have multiple IDs for one field value
		if i%3 == 0 {
			id := vars.BuildID(name + "_bis")
			user := struct{ Login, Pass string }{name, vars.BuildID(name + name + name)}
			if err := c.Put(id, user); err != nil {
				t.Error(err)
				return
			}
		}
	}

	for i := 20; i > 0; i-- {
		randInt := rand.Intn(2000)
		getAction := NewAction(Equal).CompareTo(list[randInt]).SetSelector([]string{"Login"})
		queryObj := NewQuery().SetLimit(1).Get(getAction)

		ids, err := c.Query(queryObj)
		if err != nil {
			t.Error(err)
			return
		}

		removedID := ""
		ids.Range(func(id string, _ []byte) error {
			delErr := c.Delete(id)
			if delErr != nil {
				t.Error(delErr)
				return delErr
			}
			removedID = id
			return nil
		})

		ids, err = c.Query(queryObj)
		if err != nil {
			if err != vars.ErrNotFound {
				t.Error(err.Error())
				return
			}
		}

		tmpUser := struct{ Login, Pass string }{}
		ids.Range(func(id string, retAsByte []byte) error {
			if jsonErr := json.Unmarshal(retAsByte, &tmpUser); jsonErr != nil {
				t.Error(jsonErr.Error())
				return jsonErr
			}

			if id == removedID {
				err := fmt.Errorf("this value should not be displayed")
				t.Error(err)
				return err
			}
			return nil
		})
	}
}
