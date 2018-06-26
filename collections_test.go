package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestCollection_Query(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(ctx, testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
		return
	}
	defer db.Close()

	db.SetConfig(&Conf{DefaultTransactionTimeOut * 10, DefaultQueryTimeOut * 10, DefaultInternalQueryLimit})

	c, userDBErr := db.Use("testCol")
	if userDBErr != nil {
		t.Error(userDBErr)
		return
	}

	if err := setIndexes(c); err != nil {
		t.Error(err)
		return
	}

	// Get deferent versions of dataset
	users1 := unmarshalDataSet(dataSet1)
	users2 := unmarshalDataSet(dataSet2)
	users3 := unmarshalDataSet(dataSet3)

	doneChan := make(chan error, 0)
	for i := 0; i < len(users1); i++ {
		// Inserts and updates user 2 times
		go updateUser(c, users1[i], users2[i], users3[i], doneChan)
	}
	for i := 0; i < len(users1); i++ {
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

			// i := 0
			// if n, err := gotResponse.All(func(id string, objAsBytes []byte) error {
			// 	tmpUser := new(User)
			// 	err := json.Unmarshal(objAsBytes, tmpUser)
			// 	if err != nil {
			// 		return err
			// 	}
			// 	ret[i] = tmpUser

			// 	i++
			// 	return nil
			// }); err != nil {
			// 	t.Errorf("error during range action: %s", err.Error())
			// 	return
			// } else if n != gotResponse.Len() {
			// 	t.Errorf("the response is not long %d as expected %d", n, gotResponse.Len())
			// 	return
			// }

			for i, _, v := gotResponse.First(); i >= 0; i, _, v = gotResponse.Next() {
				user := new(User)
				err := json.Unmarshal(v, user)
				if err != nil {
					t.Error(err)
					return
				}

				ret[i] = user
			}

			// for i, _, v := gotResponse.Last(); i >= 0; i, _, v = gotResponse.Prev() {
			// 	user := new(User)
			// 	err := json.Unmarshal(v, user)
			// 	if err != nil {
			// 		t.Error(err)
			// 		return
			// 	}
			// 	ret[i] = user
			// }

			// for i := 0; true; i++ {
			// 	user := new(User)
			// 	_, err := gotResponse.One(user)
			// 	if err != nil {
			// 		if err == vars.ErrTheResponseIsOver {
			// 			break
			// 		}
			// 		t.Error(err)
			// 		return
			// 	}

			// 	ret[i] = user
			// }

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
		})
	}
}

// func TestCollection_Loop_Query(t *testing.T) {
// 	if testing.Short() {
// 		t.Skip("skipping test in short mode.")
// 	}

// 	for i := 0; i < 100; i++ {
// 		if !t.Run(
// 			fmt.Sprintf("%d", i),
// 			TestCollection_Query,
// 		) {
// 			return
// 		}
// 	}
// }
