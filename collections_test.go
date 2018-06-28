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
	"github.com/fatih/structs"
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

	db.SetConfig(&Conf{DefaultTransactionTimeOut * 15, DefaultQueryTimeOut * 15, DefaultInternalQueryLimit})

	// Get deferent versions of dataset
	users := unmarshalDataSet(dataset)

	// doneChan := make(chan error, 0)
	for i := 0; i < len(users); i++ {
		err := c.Put(users[i].ID, users[i])
		if err != nil {
			t.Fatal(err)
			return nil, nil
		}
	}
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
		name    string
		args    *Query
		wantErr bool
	}{
		// string index "Email" no oreder
		{
			"Email Equal no order gödel-76",
			NewQuery().SetTimeout(time.Hour).SetLimits(10, 0).SetFilter(
				NewFilter(Equal).SetSelector("Email").
					CompareTo("gödel-76@rudolph.com"),
			),
			false,
		}, {
			"Email Greater no order f",
			NewQuery().SetFilter(
				NewFilter(Greater).SetSelector("Email").
					CompareTo("f"),
			).SetLimits(5, 0),
			false,
		}, {
			"Email Less no order k",
			NewQuery().SetFilter(
				NewFilter(Less).SetSelector("Email").
					CompareTo("k"),
			).SetLimits(5, 0),
			false,
		}, {
			"Email Between no order m to u",
			NewQuery().SetFilter(
				NewFilter(Between).SetSelector("Email").
					CompareTo("m").CompareTo("u"),
			).SetLimits(5, 0),
			false,
		},
		// string index "Email" with oreder
		{
			"Email Greater ordered by Email descendent equal wanted f",
			NewQuery().SetOrder(false, "Email").SetFilter(
				NewFilter(Greater).SetSelector("Email").EqualWanted().
					CompareTo("f"),
			).SetLimits(5, 0),
			false,
		}, {
			"Email Less ordered by Age ascendent k",
			NewQuery().SetOrder(true, "Age").SetFilter(
				NewFilter(Less).SetSelector("Email").
					CompareTo("k"),
			).SetLimits(5, 0),
			false,
		}, {
			"Email Between ordered by Age descendent equal wanted m to u",
			NewQuery().SetOrder(false, "Age").SetFilter(
				NewFilter(Between).SetSelector("Email").EqualWanted().
					CompareTo("m").CompareTo("u"),
			).SetLimits(5, 0),
			false,
		},

		// many filters
		{
			"Age 19 and a < Email < j balance and last login after 6 month ego more than 0 order by email",
			NewQuery().SetOrder(true, "Email").SetFilter(
				NewFilter(Equal).SetSelector("Age").
					CompareTo(uint(19))).SetFilter(
				NewFilter(Between).SetSelector("Email").
					CompareTo("a").CompareTo("j")).SetFilter(
				NewFilter(Greater).SetSelector("Balance").
					CompareTo(0)).SetFilter(
				NewFilter(Greater).SetSelector("LastLogin").
					CompareTo(time.Now().Add(-time.Hour * 24 * 30 * 6))),
			false,
		},

		{
			"Many Equal integer filter limit 5 order by email",
			NewQuery().SetOrder(true, "Email").SetFilter(
				NewFilter(Equal).SetSelector("Age").
					CompareTo(uint(5)),
			).SetLimits(5, 0),
			false,
		}, {
			"Greater integer filter limit 5 order by ZipCode",
			NewQuery().SetOrder(true, "Address", "ZipCode").SetFilter(
				NewFilter(Greater).SetSelector("Address", "ZipCode").EqualWanted().
					CompareTo(uint(50)),
			).SetLimits(5, 0),
			false,
		}, {
			"Less time filter limit 5 order by time",
			NewQuery().SetOrder(false, "LastLogin").SetFilter(
				NewFilter(Less).SetSelector("LastLogin").
					CompareTo(time.Now()),
			).SetLimits(5, 0),
			false,
		}, {
			"Between int filter limit 10 order by age",
			NewQuery().SetOrder(true, "Age").SetFilter(
				NewFilter(Between).SetSelector("Address", "ZipCode").
					CompareTo(uint(65)).CompareTo(uint(68)),
			).SetLimits(10, 0),
			false,
		}, {
			"Between int filter limit 10 order by email",
			NewQuery().SetOrder(true, "Email").SetFilter(
				NewFilter(Between).SetSelector("Address", "ZipCode").
					CompareTo(uint(60)).CompareTo(uint(63)),
			).SetLimits(10000, 0),
			false,
		}, {
			"Timed out",
			NewQuery().SetOrder(false, "Balance").SetFilter(
				NewFilter(Between).SetSelector("Balance").EqualWanted().
					CompareTo(-104466272306065862).CompareTo(997373309132031595),
			).SetLimits(10, 10).SetTimeout(time.Nanosecond),
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResponse, err := c.Query(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Collection.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			doQueryTest(t, gotResponse, tt.args)

			if testing.Verbose() {
				fmt.Println("")
			}
		})
	}

	// Wait after the timeout for closing the DB
	time.Sleep(time.Second)
}

func doQueryTest(t *testing.T, resp *ResponseQuery, q *Query) bool {
	if resp.Len() > q.limit {
		had := ""
		for _, responseQuery := range resp.List {
			had = fmt.Sprintf("%s\n%s", had, string(responseQuery.ContentAsBytes))
		}
		t.Errorf("returned %d objects \nHad\n%s", resp.Len(), had)
		return false
	}

	ret := make([]*User, resp.Len())

	for i, _, v := resp.First(); i >= 0; i, _, v = resp.Next() {
		if testing.Verbose() {
			fmt.Printf("%d -> %s\n", i, string(v))
		}
		user := new(User)
		err := json.Unmarshal(v, user)
		if err != nil {
			t.Error(err)
			return false
		}

		ret[i] = user
	}

	var previousToOrder interface{}

	for _, user := range ret {
		userAsStruct := structs.New(user)
		for _, filter := range q.filters {
			var field *structs.Field
			var ok bool
			for i, fieldName := range filter.selector {
				if i == 0 {
					field, ok = userAsStruct.FieldOk(fieldName)
				} else {
					field, ok = field.FieldOk(fieldName)
				}
				if !ok {
					t.Errorf("the filed %s should contain value", fieldName)
					return false
				}
			}

			valAsInterface := field.Value()
			switch v := valAsInterface.(type) {
			case string:
				switch filter.operator {
				case Equal:
					if filter.values[0].Value.(string) != v {
						t.Errorf("wrong equal value %s != %s", filter.values[0].Value.(string), v)
						return false
					}
				case Greater:
					if filter.values[0].Value.(string) >= v {
						t.Errorf("wrong greater value %s > %s", filter.values[0].Value.(string), v)
						return false
					}
				case Less:
					if filter.values[0].Value.(string) <= v {
						t.Errorf("wrong less value %s < %s", filter.values[0].Value.(string), v)
						return false
					}
				case Between:
					if filter.values[0].Value.(string) >= v || v >= filter.values[1].Value.(string) {
						t.Errorf("wrong between value %s < %s < %s", filter.values[0].Value.(string), v, filter.values[1].Value.(string))
						return false
					}
				}
			case int:
				switch filter.operator {
				case Equal:
					if filter.values[0].Value.(int) != v {
						t.Errorf("wrong equal value %d != %d", filter.values[0].Value.(int), v)
						return false
					}
				case Greater:
					if filter.values[0].Value.(int) > v {
						t.Errorf("wrong greater value %d > %d", filter.values[0].Value.(int), v)
						return false
					}
				case Less:
					if filter.values[0].Value.(int) < v {
						t.Errorf("wrong less value %d < %d", filter.values[0].Value.(int), v)
						return false
					}
				case Between:
					if filter.values[0].Value.(int) > v || v > filter.values[1].Value.(int) {
						t.Errorf("wrong between value %d < %d < %d", filter.values[0].Value.(int), v, filter.values[1].Value.(int))
						return false
					}
				}
			case uint:
				switch filter.operator {
				case Equal:
					if filter.values[0].Value.(uint) != v {
						t.Errorf("wrong equal value %d != %d", filter.values[0].Value.(uint), v)
						return false
					}
				case Greater:
					if filter.values[0].Value.(uint) > v {
						t.Errorf("wrong greater value %d > %d", filter.values[0].Value.(uint), v)
						return false
					}
				case Less:
					if filter.values[0].Value.(uint) < v {
						t.Errorf("wrong less value %d < %d", filter.values[0].Value.(uint), v)
						return false
					}
				case Between:
					if filter.values[0].Value.(uint) > v || v > filter.values[1].Value.(uint) {
						t.Errorf("wrong between value %d < %d < %d", filter.values[0].Value.(uint), v, filter.values[1].Value.(uint))
						return false
					}
				}
			case time.Time:
				switch filter.operator {
				case Equal:
					if !v.Equal(filter.values[0].Value.(time.Time)) {
						t.Errorf("wrong equal value %s != %s", filter.values[0].Value.(time.Time).String(), v.String())
						return false
					}
				case Greater:
					if v.Before(filter.values[0].Value.(time.Time)) {
						t.Errorf("wrong greater value %s > %s", filter.values[0].Value.(time.Time).String(), v.String())
						return false
					}
				case Less:
					if v.After(filter.values[0].Value.(time.Time)) {
						t.Errorf("wrong less value %s < %s", filter.values[0].Value.(time.Time).String(), v.String())
						return false
					}
				case Between:
					if v.Before(filter.values[0].Value.(time.Time)) || v.After(filter.values[1].Value.(time.Time)) {
						t.Errorf("wrong between value %s < %s < %s", filter.values[0].Value.(time.Time).String(), v.String(), filter.values[1].Value.(time.Time).String())
						return false
					}
				}
			default:
				t.Errorf("type %T not handled", v)
				return false
			}
		}

		// Start checking the order

		if len(q.orderSelector) == 0 {
			return true
		}

		var field *structs.Field
		var ok bool
		for i, fieldName := range q.orderSelector {
			if i == 0 {
				field, ok = userAsStruct.FieldOk(fieldName)
			} else {
				field, ok = field.FieldOk(fieldName)
			}
			if !ok {
				t.Errorf("the filed %s should contain value", fieldName)
				return false
			}
		}

		valAsInterface := field.Value()
		if previousToOrder == nil {
			previousToOrder = valAsInterface
			continue
		}

		switch v := valAsInterface.(type) {
		case string:
			if q.ascendent {
				if previousToOrder.(string) > v {
					t.Errorf("wrong order %s should be before %s", previousToOrder.(string), v)
				}
			} else {
				if previousToOrder.(string) < v {
					t.Errorf("wrong order %s should be before %s", v, previousToOrder.(string))
				}
			}
		case int:
			if q.ascendent {
				if previousToOrder.(int) > v {
					t.Errorf("wrong order %d should be before %d", previousToOrder.(int), v)
				}
			} else {
				if previousToOrder.(int) < v {
					t.Errorf("wrong order %d should be before %d", v, previousToOrder.(int))
				}
			}
		case uint:
			if q.ascendent {
				if previousToOrder.(uint) > v {
					t.Errorf("wrong order %d should be before %d", previousToOrder.(uint), v)
				}
			} else {
				if previousToOrder.(uint) < v {
					t.Errorf("wrong order %d should be before %d", v, previousToOrder.(uint))
				}
			}
		case time.Time:
			if q.ascendent {
				if v.Before(previousToOrder.(time.Time)) {
					t.Errorf("wrong order %s should be before %s", previousToOrder.(time.Time).String(), v.String())
				}
			} else {
				if v.After(previousToOrder.(time.Time)) {
					t.Errorf("wrong order %s should be before %s", v.String(), previousToOrder.(time.Time).String())
				}
			}
		default:
			t.Errorf("type %T not handled", v)
			return false
		}
		previousToOrder = valAsInterface
	}

	if ok := testQueryResponseReaders(t, resp, ret); !ok {
		return false
	}
	return true
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
