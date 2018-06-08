package gotinydb

import (
	cryptoRand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"os"
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

		ids, err := c.Query(queryObj)
		if err != nil {
			t.Error(err)
			return
		}
		if len(ids) != 1 {
			t.Errorf("response returned other there one ID: %v", ids)
			return
		}
	}
}

func TestStringIndexGreater(t *testing.T) {
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
		for _, id := range ids {
			user := struct{ Login, Pass string }{}
			getErr := c.Get(id, &user)
			if getErr != nil {
				t.Error(getErr.Error())
				return
			}
			// if testing.Verbose() {
			// 	fmt.Printf("user %d: %v\n", j, user)
			// }
		}
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
		for _, id := range ids {
			user := struct{ Login, Pass string }{}
			getErr := c.Get(id, &user)
			if getErr != nil {
				t.Error(getErr.Error())
				return
			}
			// if testing.Verbose() {
			// 	fmt.Printf("user %d: %v\n", j, user)
			// }
		}
	}
}

func buildRandLogins(n int) (ret []string) {
	for i := 0; i < n; i++ {
		randInt := rand.Intn(len(names))
		ret = append(ret, names[randInt])
	}
	return
}
