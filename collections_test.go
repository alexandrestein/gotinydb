package gotinydb

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestCollection_Query(t *testing.T) {
	// if testing.Short() {
	// 	t.Skip("skipping test in short mode.")
	// }

	testPath := <-getTestPathChan
	defer os.RemoveAll(testPath)
	db, openDBErr := Open(testPath)
	if openDBErr != nil {
		t.Error(openDBErr)
		return
	}
	defer db.Close()

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

	// go insertObjectsForConcurrent(c, smallDataSet3, doneChan)
	// if err, ok := <-doneChan; !ok {
	// } else if err != nil {
	// 	t.Error(err.Error())
	// 	return
	// }

	tests := []struct {
		name         string
		args         *Query
		wantResponse []*User
		wantErr      bool
	}{
		{
			"One Equal string filter limit 10",
			NewQuery().SetLimit(10).Get(
				NewFilter(Equal).SetSelector([]string{"Email"}).
					CompareTo("gödel-76@rudolph.com"),
			),
			[]*User{users3[0]},
			false,
			// }, {
			// 	"Many Equal integer filter no limit",
			// 	NewQuery().Get(
			// 		NewFilter(Equal).SetSelector([]string{"Age"}).
			// 			CompareTo(uint8(2)),
			// 	).SetLimit(5),
			// 	// []*User{users3[38], users3[174], users3[321], users3[430], users3[528], users3[545], users3[589], users3[996], users3[1026], users3[1152], users3[1164], users3[1336], users3[1389], users3[1632], users3[1688], users3[1763], users3[1850], users3[2003], users3[2007], users3[2302], users3[2458], users3[2564], users3[2663], users3[2726], users3[2743], users3[2848], users3[2951], users3[2959], users3[2997], users3[2998]},
			// 	nil,
			// 	false,
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

				fmt.Println(user)
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
				for _, responseObject := range gotResponse.List {
					wanted = fmt.Sprintf("%s\n%s", wanted, string(responseObject.ContentAsBytes))
				}
				t.Errorf("Had %s\nwant %s\n", had, wanted)
			}
		})
	}
}
