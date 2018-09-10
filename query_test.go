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
	c.SetIndex("age", UIntIndex, "Age")
	c.SetIndex("balance", IntIndex, "Balance")
	c.SetIndex("last connection", TimeIndex, "LastLogin")
	c.SetIndex("multiple level index", StringIndex, "Address", "city")
	c.SetIndex("test slice of integers", IntIndex, "related")

	c.SetIndex("never called", StringIndex, "neverMatch")

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
	c.Put(testUser.ID, testUser)

	tests := []struct {
		name         string
		args         *Query
		wantResponse []*User
		wantErr      bool
	}{
		{
			name: "Equal String Limit 1",
			args: NewQuery().SetFilter(
				NewEqualFilter("estrada-21@allie.com", "email"),
			).SetLimits(1, 0),
			wantResponse: []*User{
				{ID: "13", Email: "estrada-21@allie.com", Balance: 2923864648279932937, Address: &Address{City: "Nellie", ZipCode: 83}, Age: 10, LastLogin: mustParseTime("2016-11-20T08:59:24.779282825+01:00")},
			},
			wantErr: false,
		},
		{
			name: "Equal Int Limit 10",
			args: NewQuery().SetFilter(
				NewEqualFilter(uint(19), "Age"),
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
				NewGreaterFilter(uint(19), "Age").EqualWanted(),
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
				NewGreaterFilter(uint(19), "Age").EqualWanted(),
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
				NewLessFilter(time.Now(), "LastLogin"),
			).SetLimits(5, 0).SetOrder(false, "Age"),
			wantResponse: []*User{
				{ID: "127", Email: "prensa-56@dedekind.com", Balance: 4495937405735533066, Address: &Address{City: "Cymbeline", ZipCode: 80}, Age: 19, LastLogin: mustParseTime("2018-01-23T09:31:19.77949703+01:00")},
				{ID: "154", Email: "depp-88@christa.com", Balance: 7172349605666936298, Address: &Address{City: "Staubach", ZipCode: 50}, Age: 19, LastLogin: mustParseTime("2018-03-10T22:14:40.779554928+01:00")},
				{ID: "111", Email: "sarnoff-84@amie.com", Balance: 4059682463746307250, Address: &Address{City: "Herring", ZipCode: 58}, Age: 18, LastLogin: mustParseTime("2017-12-12T23:10:51.779471556+01:00")},
				{ID: "166", Email: "mirzam-53@carmelo.com", Balance: 4727539390372795700, Address: &Address{City: "Pribilof", ZipCode: 42}, Age: 18, LastLogin: mustParseTime("2017-10-31T22:41:07.779573284+01:00")},
				{ID: "155", Email: "gonzalez-41@gillette.com", Balance: 7233813973658996658, Address: &Address{City: "Schlesinger", ZipCode: 3}, Age: 17, LastLogin: mustParseTime("2018-03-30T23:46:47.779556355+02:00")},
			},
			wantErr: false,
		}, {
			name: "Between Int Limit 5 Order",
			args: NewQuery().SetFilter(
				NewBetweenFilter(uint(5), uint(10), "Age"),
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
				NewBetweenFilter(uint(5), uint(10), "Age").EqualWanted(),
			).SetLimits(5, 0).SetOrder(true, "Age"),
			wantResponse: []*User{
				{ID: "224", Email: "gustavus-91@godzilla.com", Balance: 157473026907431221, Address: &Address{City: "Ishim", ZipCode: 6}, Age: 5, LastLogin: mustParseTime("2017-07-24T03:46:50.779668183+02:00")},
				{ID: "217", Email: "kelli-50@scruggs.com", Balance: 6024610462857344358, Address: &Address{City: "Asperger", ZipCode: 11}, Age: 5, LastLogin: mustParseTime("2018-03-10T05:46:48.779656214+01:00")},
				{ID: "214", Email: "eugenie-68@jerrod.com", Balance: 5805317582471799175, Address: &Address{City: "Lynne", ZipCode: 18}, Age: 5, LastLogin: mustParseTime("2016-07-01T13:47:45.779651797+02:00")},
				{ID: "203", Email: "miltown-26@velez.com", Balance: 4278560190771696982, Address: &Address{City: "Eeyore", ZipCode: 67}, Age: 5, LastLogin: mustParseTime("2016-10-04T09:38:19.779635629+02:00")},
				{ID: "198", Email: "oneal-12@olive.com", Balance: 5986049212808166631, Address: &Address{City: "Laramie", ZipCode: 9}, Age: 5, LastLogin: mustParseTime("2016-09-05T13:26:01.779624982+02:00")},
			},
			wantErr: false,
		}, {
			name: "Slice query",
			args: NewQuery().SetFilter(
				NewGreaterFilter(10, "related"),
			),
			wantResponse: []*User{
				testUser,
			},
			wantErr: false,
		}, {
			name: "Exclude Filter",
			args: NewQuery().SetFilter(
				NewBetweenFilter(uint(15), uint(19), "Age"),
				NewEqualFilter(uint(17), "Age").ExclusionFilter(),
			).SetLimits(30, 0).SetOrder(false, "Age").SetTimeout(time.Hour),
			wantResponse: []*User{
				{ID: "111", Email: "sarnoff-84@amie.com", Balance: 4059682463746307250, Address: &Address{City: "Herring", ZipCode: 58}, Age: 18, LastLogin: mustParseTime("2017-12-12T23:10:51.779471556+01:00")},
				{ID: "133", Email: "citibank-38@css.com", Balance: 7414564314151778346, Address: &Address{City: "Byers", ZipCode: 49}, Age: 18, LastLogin: mustParseTime("2017-07-20T07:00:58.77950965+02:00")},
				{ID: "141", Email: "lollobrigida-65@sachs.com", Balance: -2887694497990240143, Address: &Address{City: "Loyd", ZipCode: 0}, Age: 18, LastLogin: mustParseTime("2017-07-19T22:27:58.779521946+02:00")},
				{ID: "166", Email: "mirzam-53@carmelo.com", Balance: 4727539390372795700, Address: &Address{City: "Pribilof", ZipCode: 42}, Age: 18, LastLogin: mustParseTime("2017-10-31T22:41:07.779573284+01:00")},
				{ID: "167", Email: "abrams-66@anshan.com", Balance: 5515393226037987824, Address: &Address{City: "Rene", ZipCode: 46}, Age: 18, LastLogin: mustParseTime("2016-06-28T09:07:52.779574645+02:00")},
				{ID: "205", Email: "ruby-14@benacerraf.com", Balance: 6184224934393451725, Address: &Address{City: "Malinda", ZipCode: 11}, Age: 18, LastLogin: mustParseTime("2017-02-06T06:21:05.779638538+01:00")},
				{ID: "215", Email: "sancho-18@huck.com", Balance: 5048282887095334907, Address: &Address{City: "Stu", ZipCode: 11}, Age: 18, LastLogin: mustParseTime("2016-09-10T07:10:53.779653263+02:00")},
				{ID: "226", Email: "macedon-22@haynes.com", Balance: 4476138898591813535, Address: &Address{City: "Deana", ZipCode: 96}, Age: 18, LastLogin: mustParseTime("2018-05-19T04:27:19.779671191+02:00")},
				{ID: "252", Email: "burks-55@gall.com", Balance: 6076085708776753791, Address: &Address{City: "Frazier", ZipCode: 22}, Age: 18, LastLogin: mustParseTime("2017-09-05T13:33:42.779724269+02:00")},
				{ID: "279", Email: "weinberg-70@maiman.com", Balance: 7662368864092731685, Address: &Address{City: "Sumeria", ZipCode: 44}, Age: 18, LastLogin: mustParseTime("2016-07-13T16:55:14.779772384+02:00")},
				{ID: "30", Email: "ashikaga-75@enrico.com", Balance: 8366614557447705283, Address: &Address{City: "Oneal", ZipCode: 31}, Age: 18, LastLogin: mustParseTime("2017-06-26T18:06:00.779318686+02:00")},
				{ID: "46", Email: "troilus-1@holden.com", Balance: 7322141660272070715, Address: &Address{City: "Avis", ZipCode: 76}, Age: 18, LastLogin: mustParseTime("2016-09-18T03:33:21.779344696+02:00")},
				{ID: "71", Email: "le-79@farley.com", Balance: 5517544624163890337, Address: &Address{City: "Lipton", ZipCode: 84}, Age: 18, LastLogin: mustParseTime("2016-09-27T00:03:51.779391608+02:00")},
				{ID: "75", Email: "atria-52@upton.com", Balance: 7097996370098280725, Address: &Address{City: "Maxine", ZipCode: 87}, Age: 18, LastLogin: mustParseTime("2017-05-29T08:50:29.7793998+02:00")},
				{ID: "81", Email: "carpathians-29@rudy.com", Balance: 3491889029470832159, Address: &Address{City: "Kaitlin", ZipCode: 18}, Age: 18, LastLogin: mustParseTime("2016-07-12T21:58:49.779409146+02:00")},
				{ID: "84", Email: "mckay-7@mintaka.com", Balance: 5712585702400064801, Address: &Address{City: "Ashlee", ZipCode: 57}, Age: 18, LastLogin: mustParseTime("2017-09-26T06:18:59.779414363+02:00")},
				{ID: "109", Email: "serena-76@walker.com", Balance: -3911797094884793372, Address: &Address{City: "Bobbie", ZipCode: 98}, Age: 16, LastLogin: mustParseTime("2017-10-12T15:29:31.779467973+02:00")},
				{ID: "114", Email: "bayes-78@encarta.com", Balance: -5042870714656659092, Address: &Address{City: "Peg", ZipCode: 35}, Age: 16, LastLogin: mustParseTime("2018-03-16T12:18:21.779475892+01:00")},
				{ID: "123", Email: "honecker-34@mallomars.com", Balance: 7920457748727249657, Address: &Address{City: "Iva", ZipCode: 26}, Age: 16, LastLogin: mustParseTime("2017-08-13T06:14:56.779491141+02:00")},
				{ID: "129", Email: "sargasso-25@trekkie.com", Balance: 844975672684268522, Address: &Address{City: "Effie", ZipCode: 95}, Age: 16, LastLogin: mustParseTime("2018-05-02T13:27:38.77950351+02:00")},
				{ID: "134", Email: "calderon-25@nosferatu.com", Balance: 6860872600828982654, Address: &Address{City: "Willis", ZipCode: 19}, Age: 16, LastLogin: mustParseTime("2017-03-03T07:36:17.779511263+01:00")},
				{ID: "143", Email: "rowland-96@sihanouk.com", Balance: 5198477152645610974, Address: &Address{City: "Surya", ZipCode: 88}, Age: 16, LastLogin: mustParseTime("2018-02-23T10:49:59.779524702+01:00")},
				{ID: "151", Email: "ramos-70@eleazar.com", Balance: 7355085083107302895, Address: &Address{City: "Schiaparelli", ZipCode: 95}, Age: 16, LastLogin: mustParseTime("2017-07-07T07:50:50.779550626+02:00")},
				{ID: "16", Email: "sony-85@pynchon.com", Balance: 1241694766499487797, Address: &Address{City: "Ernie", ZipCode: 12}, Age: 16, LastLogin: mustParseTime("2018-03-22T13:08:26.779287787+01:00")},
				{ID: "172", Email: "tide-13@traci.com", Balance: 4385233418122245928, Address: &Address{City: "Jaclyn", ZipCode: 40}, Age: 16, LastLogin: mustParseTime("2016-09-06T10:25:26.779581821+02:00")},
				{ID: "19", Email: "elvia-78@joy.com", Balance: 433762869939193238, Address: &Address{City: "Samuelson", ZipCode: 47}, Age: 16, LastLogin: mustParseTime("2018-04-02T21:21:33.779294954+02:00")},
				{ID: "194", Email: "kelsey-8@vijayawada.com", Balance: 6322222817173288818, Address: &Address{City: "Lamont", ZipCode: 71}, Age: 16, LastLogin: mustParseTime("2017-10-25T09:16:41.779619116+02:00")},
				{ID: "211", Email: "alex-45@toynbee.com", Balance: 2562818978838785098, Address: &Address{City: "Claudio", ZipCode: 93}, Age: 16, LastLogin: mustParseTime("2017-04-15T06:11:00.779647659+02:00")},
				{ID: "212", Email: "tabitha-90@amadeus.com", Balance: 3328391776107726397, Address: &Address{City: "Hermosillo", ZipCode: 73}, Age: 16, LastLogin: mustParseTime("2016-07-14T18:14:45.779649013+02:00")},
				{ID: "230", Email: "rydberg-95@domingo.com", Balance: 2061015192054309365, Address: &Address{City: "Carr", ZipCode: 10}, Age: 16, LastLogin: mustParseTime("2016-08-08T21:31:45.77967697+02:00")},
			},
			wantErr: false,
		},
		{
			name: "Nothing To Do",
			args: NewQuery(),
			wantResponse: []*User{
				testUser,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResponse, err := c.Query(tt.args)

			if (err != nil) != tt.wantErr {
				t.Errorf("Collection.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
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

	_, err = c.Query(
		NewQuery().SetFilter(
			NewEqualFilter("estrada-21@allie.com", "email"),
		).SetLimits(1, 0).SetTimeout(time.Nanosecond),
	)
	if err == nil {
		t.Errorf("the query must timeout")
	}

	queryResponses(t, c)
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

func queryResponses(t *testing.T, c *Collection) {
	expectedResponseList := []*User{
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
	}

	responseQuery, _ := c.Query(
		NewQuery().SetFilter(
			NewEqualFilter(uint(19), "Age"),
		).SetLimits(10, 0),
	)
	for i, _, v := responseQuery.First(); i >= 0; i, _, v = responseQuery.Next() {
		tmpObj := new(Type)
		json.Unmarshal(v, tmpObj)

		if reflect.DeepEqual(tmpObj, expectedResponseList[i]) {
			t.Errorf("not the expected response: \n\t%v\n\t%v", tmpObj, expectedResponseList[i])
			return
		}
	}

	responseQuery, _ = c.Query(
		NewQuery().SetFilter(
			NewEqualFilter(uint(19), "Age"),
		).SetLimits(10, 0),
	)
	for i, _, v := responseQuery.Last(); i >= 0; i, _, v = responseQuery.Prev() {
		tmpObj := new(Type)
		json.Unmarshal(v, tmpObj)

		if reflect.DeepEqual(tmpObj, expectedResponseList[i]) {
			t.Errorf("not the expected response: \n\t%v\n\t%v", tmpObj, expectedResponseList[i])
			return
		}
	}

	responseQuery, _ = c.Query(
		NewQuery().SetFilter(
			NewEqualFilter(uint(19), "Age"),
		).SetLimits(10, 0),
	)
	_, err := responseQuery.All(func(id string, contentAsBytes []byte) error {
		if id == "11" {
			return fmt.Errorf("fake")
		}
		return nil
	})
	if err == nil {
		t.Errorf("the All function should return the \"fake\" error")
		return
	}

	responseQuery, _ = c.Query(
		NewQuery().SetFilter(
			NewEqualFilter("estrada-21@allie.com", "email"),
		),
	)
	tmpU := new(User)
	_, err = responseQuery.One(tmpU)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = responseQuery.One(tmpU)
	if err != ErrResponseOver {
		t.Errorf("the response is over and an error must be returned")
		return
	}
}

func TestIndexInfo(t *testing.T) {
	ii := new(IndexInfo)

	ii.Type = StringIndex
	if ii.GetType() != StringIndexString {
		t.Errorf("expected %s but had %s", StringIndexString, ii.GetType())
	}

	ii.Type = IntIndex
	if ii.GetType() != IntIndexString {
		t.Errorf("expected %s but had %s", IntIndexString, ii.GetType())
	}

	ii.Type = UIntIndex
	if ii.GetType() != UIntIndexString {
		t.Errorf("expected %s but had %s", UIntIndexString, ii.GetType())
	}

	ii.Type = TimeIndex
	if ii.GetType() != TimeIndexString {
		t.Errorf("expected %s but had %s", TimeIndexString, ii.GetType())
	}

	ii.Type = -1
	if ii.GetType() != "" {
		t.Errorf("expected empty string but had %s", ii.GetType())
	}
}

func TestIDType(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	id := newID(ctx, "id")
	id.treeItem()
}
