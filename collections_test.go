package gotinydb

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestCollection_Query(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

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

	doneChan := make(chan error, 0)

	// users1 := unmarshalDataSet(dataSet1)
	// users2 := unmarshalDataSet(dataSet2)
	users3 := unmarshalDataSet(dataSet3)
	// for i := range users1 {
	// 	go updateUser(c, users1[i], users2[i], users3[i], doneChan)
	// }
	// for i := 0; i < len(users1)-1; i++ {
	// 	err := <-doneChan
	// 	if err != nil {
	// 		t.Error(err)
	// 		return
	// 	}
	// }

	go insertObjectsForConcurrent(c, dataSet3, doneChan)
	<-doneChan

	tests := []struct {
		name         string
		args         *Query
		wantResponse []*User
		wantErr      bool
	}{
		{
			"One Equal string filter limit 1",
			NewQuery().SetLimit(1).Get(
				NewFilter(Equal).SetSelector([]string{"Email"}).CompareTo("Jeanette-2829@Molnar.com"),
			),
			[]*User{users3[2829]},
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
				t.Errorf("returned %d objects but the expected %d", gotResponse.Len(), len(tt.wantResponse))
				return
			}

			ret := make([]*User, gotResponse.Len())

			// for _, _, v := gotResponse.First(); v != nil; _, _, v = gotResponse.Next() {
			// 	user := new(User)
			// 	err := json.Unmarshal(v, user)
			// 	if err != nil {
			// 		t.Error(err)
			// 		return
			// 	}
			// 	ret = append(ret, user)
			// }

			for i := 0; i < gotResponse.Len(); i++ {
				user := new(User)
				notOver, err := gotResponse.One(user)
				if err != nil {
					t.Error(err)
					return
				}
				if !notOver {
					break
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
				for _, objecyAsBytes := range gotResponse.ObjectsAsBytes {
					wanted = fmt.Sprintf("%s\n%s", wanted, string(objecyAsBytes))
				}
				t.Errorf("Collection.Query() = %s\n, want %s\n", had, wanted)
			}
		})
	}
}
