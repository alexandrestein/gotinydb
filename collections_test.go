package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alexandrestein/gotinydb/vars"
)

func queryFillUp(ctx context.Context, t *testing.T, dataset []byte) (*DB, []*User) {
	testPath := <-getTestPathChan

	db, openDBErr := Open(ctx, testPath)
	if openDBErr != nil {
		t.Fatal(openDBErr)
		return nil, nil
	}

	c, userDBErr := db.Use("testCol")
	if userDBErr != nil {
		t.Fatal(userDBErr)
		return nil, nil
	}

	if err := setIndexes(c); err != nil {
		t.Fatal(err)
		return nil, nil
	}

	db.SetConfig(&Conf{DefaultTransactionTimeOut * 100, DefaultQueryTimeOut * 100, DefaultInternalQueryLimit})

	// Get deferent versions of dataset
	users := unmarshalDataSet(dataset)

	// doneChan := make(chan error, 0)
	for i := 0; i < len(users); i++ {
		err := c.Put(users[i].ID, users[i])
		if err != nil {
			t.Fatal(err)
			return nil, nil
		}
		// Inserts and updates user 2 times
		// go updateUser(c, users1[i], users2[i], users[i], doneChan)
	}
	// for i := 0; i < len(users); i++ {
	// 	err := <-doneChan
	// 	if err != nil {
	// 		t.Fatal(err)
	// 		return nil, nil
	// 	}
	// }
	return db, users
}

func TestCollection_Query(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, users := queryFillUp(ctx, t, dataSet1)
	if db == nil {
		return
	}
	defer db.Close()
	defer os.RemoveAll(db.Path)

	c, userDBErr := db.Use("testCol")
	if userDBErr != nil {
		t.Error(userDBErr)
		return
	}

	users2 := unmarshalDataSet(dataSet2)
	users3 := unmarshalDataSet(dataSet3)

	doneChan := make(chan error, 0)
	for i := 0; i < len(users); i++ {
		// Inserts and updates user 2 times
		go updateUser(c, users[i], users2[i], users3[i], doneChan)
	}
	for i := 0; i < len(users); i++ {
		err := <-doneChan
		if err != nil {
			t.Error(err)
			return
		}
	}

	tests := []struct {
		name         string
		args         *Query
		wantResponse []*User
		wantErr      bool
	}{
		{
			"One Equal string filter limit 10",
			NewQuery().SetLimits(10, 0).Get(
				NewFilter(Equal).SetSelector([]string{"Email"}).
					CompareTo("gödel-76@rudolph.com"),
			),
			[]*User{users3[0]},
			false,
		}, {
			"Many Equal integer filter limit 5 order by email",
			NewQuery().SetOrder([]string{"Email"}, true).Get(
				NewFilter(Equal).SetSelector([]string{"Age"}).
					CompareTo(uint8(5)),
			).SetLimits(5, 0),
			[]*User{users3[144], users3[35], users3[178], users3[214], users3[224]},
			false,
		}, {
			"Greater integer filter limit 5 order by ZipCode",
			NewQuery().SetOrder([]string{"Address", "ZipCode"}, true).Get(
				NewFilter(Greater).SetSelector([]string{"Address", "ZipCode"}).EqualWanted().
					CompareTo(uint(50)),
			).SetLimits(5, 0),
			[]*User{users3[130], users3[154], users3[260], users3[264], users3[107]},
			false,
		}, {
			"Less time filter limit 5 order by time",
			NewQuery().SetOrder([]string{"LastLogin"}, false).Get(
				NewFilter(Less).SetSelector([]string{"LastLogin"}).
					CompareTo(time.Now()),
			).SetLimits(5, 0),
			[]*User{users3[10], users3[129], users3[132], users3[108], users3[120]},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResponse, err := c.Query(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Collection.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotResponse.Len() != len(tt.wantResponse) {
				t.Errorf("returned %d objects but the expected %d\n%v", gotResponse.Len(), len(tt.wantResponse), gotResponse.List)
				return
			}

			ret := make([]*User, gotResponse.Len())

			for i, _, v := gotResponse.First(); i >= 0; i, _, v = gotResponse.Next() {
				user := new(User)
				err := json.Unmarshal(v, user)
				if err != nil {
					t.Error(err)
					return
				}

				ret[i] = user
			}

			if !reflect.DeepEqual(ret, tt.wantResponse) {
				had := ""
				for _, user := range ret {
					userAsJSON, _ := json.Marshal(user)
					had = fmt.Sprintf("%s\n%s", had, string(userAsJSON))
				}
				wanted := ""
				for _, wantedValue := range tt.wantResponse {
					wantedValueAsBytes, _ := json.Marshal(wantedValue)
					wanted = fmt.Sprintf("%s\n%s", wanted, string(wantedValueAsBytes))
				}
				t.Errorf("Had %s\nwant %s\n", had, wanted)
			}

			if ok := testQueryResponseReaders(t, gotResponse, ret); !ok {
				return
			}
		})
	}
}

func testQueryResponseReaders(t *testing.T, response *ResponseQuery, checkRet []*User) bool {
	ret := make([]*User, response.Len())
	// Use the All function to get the result into object
	i := 0
	if n, err := response.All(func(id string, objAsBytes []byte) error {
		tmpUser := new(User)
		err := json.Unmarshal(objAsBytes, tmpUser)
		if err != nil {
			return err
		}
		ret[i] = tmpUser

		i++
		return nil
	}); err != nil {
		t.Errorf("error during range action: %s", err.Error())
		return false
	} else if n != response.Len() {
		t.Errorf("the response is not long %d as expected %d", n, response.Len())
		return false
	}
	if !checkExtractResultEqualToWantedResult(t, ret, checkRet) {
		return false
	}

	// List all result from the first to the last with the next function
	for i, _, v := response.First(); i >= 0; i, _, v = response.Next() {
		user := new(User)
		err := json.Unmarshal(v, user)
		if err != nil {
			t.Error(err)
			return false
		}

		ret[i] = user
	}
	if !checkExtractResultEqualToWantedResult(t, ret, checkRet) {
		return false
	}

	// List all result from the last to the fist with the prev function
	for i, _, v := response.Last(); i >= 0; i, _, v = response.Prev() {
		user := new(User)
		err := json.Unmarshal(v, user)
		if err != nil {
			t.Error(err)
			return false
		}
		ret[i] = user
	}
	if !checkExtractResultEqualToWantedResult(t, ret, checkRet) {
		return false
	}

	// Use the function One to get the users one after the other
	for i := 0; true; i++ {
		user := new(User)
		_, err := response.One(user)
		if err != nil {
			if err == vars.ErrTheResponseIsOver {
				break
			}
			t.Error(err)
			return false
		}

		ret[i] = user
	}
	if !checkExtractResultEqualToWantedResult(t, ret, checkRet) {
		return false
	}

	return true
}

func checkExtractResultEqualToWantedResult(t *testing.T, givenRet, checkRet []*User) bool {
	if !reflect.DeepEqual(givenRet, checkRet) {
		t.Errorf("The response is not the same as the one send by the checker")
		return false
	}
	return true
}

func TestCollection_Delete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, _ := queryFillUp(ctx, t, dataSet1)
	if db == nil {
		return
	}
	defer db.Close()
	defer os.RemoveAll(db.Path)

	if err := db.DeleteCollection("testCol"); err != nil {
		t.Error(err)
		return
	}
}
