package gotinydb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCollection_Query(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()

	testPath := "queryTest"
	defer os.RemoveAll(testPath)

	conf := NewDefaultOptions(testPath)
	// This limit the queue to prevent the race overflow during test
	conf.PutBufferLimit = 50

	db, err := Open(ctx, conf)
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

	c.SetIndex("email", StringIndex, "email")
	c.SetIndex("age", IntIndex, "Age")
	c.SetIndex("last connection", TimeIndex, "LastLogin")

	// Insert element in concurrent way to test the index system
	for _, dataset := range []dataset{dataset1, dataset2, dataset3} {
		var wg sync.WaitGroup
		for _, user := range unmarshalDataset(dataset) {
			wg.Add(1)
			go func(c *Collection, user *User) {
				err := c.Put(user.ID, user)
				if err != nil {
					t.Error(err)
					return
				}
				wg.Done()
			}(c, user)
		}

		wg.Wait()
	}

	tests := []struct {
		name         string
		args         *Query
		wantResponse []*User
		wantErr      bool
	}{
		{
			name: "Equal String Limit 1",
			args: NewQuery().SetFilter(
				NewFilter(Equal).CompareTo("estrada-21@allie.com").SetSelector("email"),
			).SetLimits(1, 0),
			wantResponse: []*User{
				{ID: "13", Email: "estrada-21@allie.com", Balance: 2923864648279932937, Address: &Address{City: "Nellie", ZipCode: 83}, Age: 10, LastLogin: mustParseTime("2016-11-20T08:59:24.779282825+01:00")},
			},
			wantErr: false,
		},
		{
			name: "Equal Int Limit 10",
			args: NewQuery().SetFilter(
				NewFilter(Equal).CompareTo(uint(19)).SetSelector("Age"),
			).SetLimits(10, 0),
			wantResponse: []*User{
				{ID: "100", Email: "ferguson-85@leroy.com", Balance: 4447977972181900834, Address: &Address{City: "Maserati", ZipCode: 56}, Age: 19, LastLogin: mustParseTime("2017-03-17T17:38:44.77944231+01:00")},
				{ID: "107", Email: "huntington-25@selectric.com", Balance: 8348423686594443311, Address: &Address{City: "Clarice", ZipCode: 51}, Age: 19, LastLogin: mustParseTime("2017-08-14T01:12:14.779462865+02:00")},
				{ID: "11", Email: "vesalius-32@rowland.com", Balance: 3550438342716738513, Address: &Address{City: "Mazola", ZipCode: 97}, Age: 19, LastLogin: mustParseTime("2016-08-03T00:56:02.779279525+02:00")},
				{ID: "119", Email: "hofstadter-76@oranjestad.com", Balance: 2241106655799453235, Address: &Address{City: "Rodriguez", ZipCode: 90}, Age: 19, LastLogin: mustParseTime("2017-03-07T01:18:58.77948378+01:00")},
				{ID: "127", Email: "prensa-56@dedekind.com", Balance: 4495937405735533066, Address: &Address{City: "Cymbeline", ZipCode: 80}, Age: 19, LastLogin: mustParseTime("2018-01-23T09:31:19.77949703+01:00")},
				{ID: "14", Email: "wigner-79@salisbury.com", Balance: 3372068499639092378, Address: &Address{City: "Roscoe", ZipCode: 7}, Age: 19, LastLogin: mustParseTime("2017-03-10T11:26:43.77928466+01:00")},
				{ID: "154", Email: "depp-88@christa.com", Balance: 7172349605666936298, Address: &Address{City: "Staubach", ZipCode: 50}, Age: 19, LastLogin: mustParseTime("2018-03-10T22:14:40.779554928+01:00")},
				{ID: "186", Email: "philippe-62@sellers.com", Balance: 6945390690498606599, Address: &Address{City: "Aurelio", ZipCode: 20}, Age: 19, LastLogin: mustParseTime("2016-07-22T10:29:35.779602878+02:00")},
				{ID: "188", Email: "michelob-87@loyd.com", Balance: 8819151968236029214, Address: &Address{City: "Alisha", ZipCode: 89}, Age: 19, LastLogin: mustParseTime("2017-07-15T17:55:02.779607813+02:00")},
				{ID: "193", Email: "bela-24@stephanie.com", Balance: 7418882447566429223, Address: &Address{City: "Lazaro", ZipCode: 43}, Age: 19, LastLogin: mustParseTime("2017-08-14T20:31:52.779617536+02:00")},
			},
			wantErr: false,
		}, {
			name: "Greater Int Limit 5 No Order",
			args: NewQuery().SetFilter(
				NewFilter(Greater).CompareTo(uint(19)).SetSelector("Age").EqualWanted(),
			).SetLimits(5, 0),
			wantResponse: []*User{
				{ID: "100", Email: "ferguson-85@leroy.com", Balance: 4447977972181900834, Address: &Address{City: "Maserati", ZipCode: 56}, Age: 19, LastLogin: mustParseTime("2017-03-17T17:38:44.77944231+01:00")},
				{ID: "107", Email: "huntington-25@selectric.com", Balance: 8348423686594443311, Address: &Address{City: "Clarice", ZipCode: 51}, Age: 19, LastLogin: mustParseTime("2017-08-14T01:12:14.779462865+02:00")},
				{ID: "11", Email: "vesalius-32@rowland.com", Balance: 3550438342716738513, Address: &Address{City: "Mazola", ZipCode: 97}, Age: 19, LastLogin: mustParseTime("2016-08-03T00:56:02.779279525+02:00")},
				{ID: "119", Email: "hofstadter-76@oranjestad.com", Balance: 2241106655799453235, Address: &Address{City: "Rodriguez", ZipCode: 90}, Age: 19, LastLogin: mustParseTime("2017-03-07T01:18:58.77948378+01:00")},
				{ID: "127", Email: "prensa-56@dedekind.com", Balance: 4495937405735533066, Address: &Address{City: "Cymbeline", ZipCode: 80}, Age: 19, LastLogin: mustParseTime("2018-01-23T09:31:19.77949703+01:00")},
			},
			wantErr: false,
		}, {
			name: "Greater Int Limit 5 With Order",
			args: NewQuery().SetFilter(
				NewFilter(Greater).CompareTo(uint(19)).SetSelector("Age").EqualWanted(),
			).SetLimits(5, 0).SetOrder(true, "Age"),
			wantResponse: []*User{
				{ID: "79", Email: "katharine-15@torres.com", Balance: 6167440685671547817, Address: &Address{City: "Callie", ZipCode: 31}, Age: 19, LastLogin: mustParseTime("2017-06-20T18:35:28.779405337+02:00")},
				{ID: "63", Email: "flowers-35@gelbvieh.com", Balance: 2953156017263370109, Address: &Address{City: "Daugherty", ZipCode: 19}, Age: 19, LastLogin: mustParseTime("2016-08-27T06:51:39.779372769+02:00")},
				{ID: "52", Email: "elasticsearch-53@allan.com", Balance: 7987054792940577791, Address: &Address{City: "Barrera", ZipCode: 79}, Age: 19, LastLogin: mustParseTime("2017-01-04T02:53:25.779356238+01:00")},
				{ID: "45", Email: "manfred-10@longstreet.com", Balance: 1679042353910982691, Address: &Address{City: "Ronda", ZipCode: 2}, Age: 19, LastLogin: mustParseTime("2017-02-06T13:49:18.779343193+01:00")},
				{ID: "43", Email: "weiss-28@chrystal.com", Balance: 1731454522748100641, Address: &Address{City: "Atacama", ZipCode: 94}, Age: 19, LastLogin: mustParseTime("2017-12-20T04:33:57.779340208+01:00")},
			},
			wantErr: false,
		}, {
			name: "Less Time Limit 5 With Order",
			args: NewQuery().SetFilter(
				NewFilter(Less).CompareTo(time.Now()).SetSelector("LastLogin"),
			).SetLimits(5, 0).SetOrder(false, "LastLogin"),
			wantResponse: []*User{
				{ID: "174", Email: "woodstock-67@lavonne.com", Balance: -7207283129708867248, Address: &Address{City: "Kroger", ZipCode: 88}, Age: 17, LastLogin: mustParseTime("2018-06-19T02:03:53.779584878+02:00")},
				{ID: "182", Email: "travolta-38@atkins.com", Balance: -657472957981999921, Address: &Address{City: "McFarland", ZipCode: 27}, Age: 3, LastLogin: mustParseTime("2018-06-14T03:55:14.779596482+02:00")},
				{ID: "10", Email: "glastonbury-16@ferguson.com", Balance: 2238062777506547327, Address: &Address{City: "Lyme", ZipCode: 92}, Age: 12, LastLogin: mustParseTime("2018-05-26T20:10:17.779277588+02:00")},
				{ID: "20", Email: "bloomingdale-95@baidu.com", Balance: 5734714954518056234, Address: &Address{City: "Rosales", ZipCode: 36}, Age: 1, LastLogin: mustParseTime("2018-05-26T11:18:33.779297231+02:00")},
				{ID: "152", Email: "clementine-18@beeton.com", Balance: 5159224772251481170, Address: &Address{City: "Eloise", ZipCode: 0}, Age: 4, LastLogin: mustParseTime("2018-05-21T05:27:47.779552052+02:00")},
			},
			wantErr: false,
		}, {
			name: "Between Int Limit 5 Order",
			args: NewQuery().SetFilter(
				NewFilter(Between).CompareTo(uint(5)).CompareTo(uint(10)).SetSelector("Age"),
			).SetLimits(5, 0).SetOrder(true, "Age"),
			wantResponse: []*User{
				{ID: "283", Email: "bonhoeffer-67@agatha.com", Balance: 6915424560435208594, Address: &Address{City: "Lon", ZipCode: 96}, Age: 6, LastLogin: mustParseTime("2016-12-15T19:17:56.779778796+01:00")},
				{ID: "238", Email: "sweeney-44@maserati.com", Balance: 2921351223480399877, Address: &Address{City: "Ahmadinejad", ZipCode: 93}, Age: 6, LastLogin: mustParseTime("2016-08-08T00:20:44.779696507+02:00")},
				{ID: "236", Email: "jerrold-8@figueroa.com", Balance: 1057898234992247733, Address: &Address{City: "Claudette", ZipCode: 98}, Age: 6, LastLogin: mustParseTime("2017-11-03T19:32:57.779693564+01:00")},
				{ID: "227", Email: "lynda-24@zappa.com", Balance: 5425268988844687454, Address: &Address{City: "Palisades", ZipCode: 87}, Age: 6, LastLogin: mustParseTime("2017-03-17T03:29:00.779672591+01:00")},
				{ID: "208", Email: "belinda-9@bullwinkle.com", Balance: 2259971995563681914, Address: &Address{City: "Amaru", ZipCode: 95}, Age: 6, LastLogin: mustParseTime("2017-01-17T20:01:07.779642818+01:00")},
			},
			wantErr: false,
		}, {
			name: "Between Int Limit 5 Equal Wanted Order",
			args: NewQuery().SetFilter(
				NewFilter(Between).CompareTo(uint(5)).CompareTo(uint(10)).SetSelector("Age").EqualWanted(),
			).SetLimits(5, 0).SetOrder(true, "Age"),
			wantResponse: []*User{
				{ID: "224", Email: "gustavus-91@godzilla.com", Balance: 157473026907431221, Address: &Address{City: "Ishim", ZipCode: 6}, Age: 5, LastLogin: mustParseTime("2017-07-24T03:46:50.779668183+02:00")},
				{ID: "217", Email: "kelli-50@scruggs.com", Balance: 6024610462857344358, Address: &Address{City: "Asperger", ZipCode: 11}, Age: 5, LastLogin: mustParseTime("2018-03-10T05:46:48.779656214+01:00")},
				{ID: "214", Email: "eugenie-68@jerrod.com", Balance: 5805317582471799175, Address: &Address{City: "Lynne", ZipCode: 18}, Age: 5, LastLogin: mustParseTime("2016-07-01T13:47:45.779651797+02:00")},
				{ID: "203", Email: "miltown-26@velez.com", Balance: 4278560190771696982, Address: &Address{City: "Eeyore", ZipCode: 67}, Age: 5, LastLogin: mustParseTime("2016-10-04T09:38:19.779635629+02:00")},
				{ID: "198", Email: "oneal-12@olive.com", Balance: 5986049212808166631, Address: &Address{City: "Laramie", ZipCode: 9}, Age: 5, LastLogin: mustParseTime("2016-09-05T13:26:01.779624982+02:00")},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResponse, err := c.Query(tt.args)

			if (err != nil) != tt.wantErr {
				t.Errorf("Collection.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			users := make([]*User, gotResponse.Len())
			i := 0
			if _, err := gotResponse.All(func(id string, objAsBytes []byte) error {
				tmpObj := new(User)
				err := json.Unmarshal(objAsBytes, tmpObj)
				if err != nil {
					return err
				}
				// Add the element into the slice
				users[i] = tmpObj

				i++
				return nil
			}); err != nil {
				t.Error(err)
				return
			}

			if !reflect.DeepEqual(users, tt.wantResponse) {
				t.Errorf("\n%v\n%v", printSliceOfUsers(users), printSliceOfUsers(tt.wantResponse))
			}
		})
	}
}

func mustParseTime(input string) time.Time {
	t, _ := time.Parse(time.RFC3339, input)
	return t
}

func printSliceOfUsers(input []*User) (ret string) {
	for i, user := range input {
		ret = fmt.Sprintf("%s%d\t%s\n", ret, i, user.String())
	}
	return
}
