package gotinydb

// func TestDebug(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
// 	defer cancel()

// 	testPath := "debugTest"
// 	defer os.RemoveAll(testPath)

// 	conf := NewDefaultOptions(testPath)
// 	db, err := Open(ctx, conf)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	defer db.Close()

// 	c, err := db.Use("user collection")
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	c.SetIndex("email", StringIndex, "email")
// 	c.SetIndex("age", UIntIndex, "Age")
// 	c.SetIndex("balance", IntIndex, "Balance")
// 	c.SetIndex("last connection", TimeIndex, "LastLogin")
// 	c.SetIndex("multiple level index", StringIndex, "Address", "city")
// 	c.SetIndex("test slice of integers", IntIndex, "related")

// 	// c.SetIndex("never called", StringIndex, "neverMatch")

// 	err = c.Put(testUser.ID, testUser)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	testUserBis := *testUser
// 	testUserBis.Age = uint(20)
// 	err = c.Put(testUser.ID, &testUserBis)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	var response *Response
// 	response, err = c.Query(NewQuery().SetFilter(NewLessFilter(uint(100), "Age")))
// 	// response, err = c.Query(NewQuery().SetFilter(NewGreaterFilter(uint(18), "Age")))
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	getUser := new(User)
// 	var id string
// 	id, err = response.One(getUser)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}

// 	fmt.Println("OK", id, getUser)
// }
