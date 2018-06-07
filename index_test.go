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

	index := new(Index)
	index.Name = "test index"
	index.Selector = []string{"Login"}
	index.Type = vars.StringIndex
	c.SetIndex(index)

	list := buildRandLogins(2000)
	for _, name := range list {
		id := vars.BuildID(name)
		user := struct{ Login, Pass string }{name, vars.BuildID(name + name)}
		if err := c.Put(id, user); err != nil {
			t.Error(err)
			return
		}
	}

	for i := 2000; i > 0; i-- {
		randInt := rand.Intn(2000)
		getAction := NewAction(Equal).CompareTo(list[randInt]).SetSelector([]string{"Login"})
		queryObj := NewQuery().SetLimit(1).Get(getAction)

		ids := c.Query(queryObj)
		if len(ids) != 1 {
			t.Errorf("response retured other there one ID: %v", ids)
			return
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
