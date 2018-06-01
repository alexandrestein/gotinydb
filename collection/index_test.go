package GoTinyDB

import (
	"reflect"
	"testing"

	internalTesting "github.com/alexandreStein/GoTinyDB/testing"
	"github.com/alexandreStein/GoTinyDB/vars"

	"github.com/alexandreStein/gods/utils"
	bolt "github.com/coreos/bbolt"
)

func TestStringQuery(t *testing.T) {
	selector := []string{"Add", "Street", "Name"}
	colName := "collectionTest"
	db, _ := bolt.Open(internalTesting.Path, vars.FilePermission, nil)
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucket(vars.InternalBuckectCollections)
		metaBucket, _ := tx.CreateBucket(vars.InternalBuckectMetaDatas)
		metaBucket.CreateBucket([]byte(colName))
		return nil
	})
	// i := index.NewStringIndex(internalTesting.Path, selector)
	col := NewCollection(db, colName)
	col.SetIndex("streetName", utils.StringComparatorType, selector)

	for _, val := range internalTesting.GetCompleteUsersExampleStreetNamesOnly() {
		user := val.(*internalTesting.CompleteUser)
		col.Put(user.GetID(), user.GetContent())
	}

	buildTestQuery := func(limit int, reverted bool, getActions, keepActions []*Action) *Query {
		if limit == 0 {
			limit = 1
		}
		q := NewQuery().SetLimit(limit)
		if reverted {
			q.InvertOrder()
		}
		if getActions != nil {
			for _, action := range getActions {
				action.Selector = selector
			}
		}
		q.GetActions = getActions
		if keepActions != nil {
			for _, action := range keepActions {
				action.Selector = selector
			}
		}
		q.KeepActions = keepActions
		return q
	}

	listOfTests := []struct {
		name           string
		query          *Query
		expectedResult []string
	}{
		{
			name: "Get Equal with limit 1",
			query: buildTestQuery(1, false,
				[]*Action{NewAction(Equal).CompareTo("North street")},
				nil),
			expectedResult: []string{"S_North_1"},
		}, {
			name: "Get Equal with limit 5",
			query: buildTestQuery(5, false,
				[]*Action{NewAction(Equal).CompareTo("North street")},
				nil),
			expectedResult: []string{"S_North_1", "S_North_6", "S_North_11", "S_North_16", "S_North_21"},
		}, {
			name: "Get Equal with limit 5 and reverted",
			query: buildTestQuery(5, true,
				[]*Action{NewAction(Equal).CompareTo("North street")},
				nil),
			expectedResult: []string{"S_North_21", "S_North_16", "S_North_11", "S_North_6", "S_North_1"},
		}, {
			name: "Get Greater and Equal - limit 15",
			query: buildTestQuery(15, false,
				[]*Action{NewAction(Greater).CompareTo("East street")},
				nil),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49", "S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25"},
		}, {
			name: "Get Greater - limit 10",
			query: buildTestQuery(10, false,
				[]*Action{NewAction(Greater).CompareTo("East street").EqualWanted()},
				nil),
			expectedResult: []string{"S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25", "S_George_30", "S_George_35", "S_George_40", "S_George_45", "S_North_1"},
		}, {
			name: "Get Less and Equal - limit 15",
			query: buildTestQuery(15, false,
				[]*Action{NewAction(Less).CompareTo("West street")},
				nil),
			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23", "S_West_28", "S_West_33", "S_West_38", "S_West_43", "S_West_48", "S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22"},
		}, {
			name: "Get Less - limit 10",
			query: buildTestQuery(10, false,
				[]*Action{NewAction(Less).CompareTo("West street").EqualWanted()},
				nil),
			expectedResult: []string{"S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22", "S_South_27", "S_South_32", "S_South_37", "S_South_42", "S_South_47"},
		}, {
			name:           "Empty Action",
			query:          buildTestQuery(10, false, nil, nil),
			expectedResult: []string{},
		}, {
			name: "Greater than last",
			query: buildTestQuery(5, false,
				[]*Action{NewAction(Greater).CompareTo("Z")},
				nil),
			expectedResult: []string{},
		}, {
			name: "Less than first",
			query: buildTestQuery(5, false,
				[]*Action{NewAction(Less).CompareTo("A")},
				nil),
			expectedResult: []string{},
		}, {
			name: "Greater from start",
			query: buildTestQuery(5, false,
				[]*Action{NewAction(Greater).CompareTo("A")},
				nil),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24"},
		}, {
			name: "Less from end",
			query: buildTestQuery(5, false,
				[]*Action{NewAction(Less).CompareTo("Z")},
				nil),
			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23"},
		}, {
			name: "Greater from start and keep after E - limit 20",
			query: buildTestQuery(100, false,
				[]*Action{NewAction(Greater).CompareTo("A")},
				[]*Action{NewAction(Greater).CompareTo("F")}),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49"},
		}, {
			name: "Duplicated",
			query: buildTestQuery(10, false,
				[]*Action{NewAction(Equal).CompareTo("North street Dup")},
				[]*Action{NewAction(Greater).CompareTo("North street Dup4")}).DistinctWanted(),
			expectedResult: []string{"DUP_1"},
		},
	}

	for _, test := range listOfTests {
		ids := col.Query(test.query)
		if !reflect.DeepEqual(test.expectedResult, ids) {
			if len(test.expectedResult) == 0 && len(ids) == 0 {
				continue
			}
			t.Errorf("%q the expected result is %v but had %v", test.name, test.expectedResult, ids)
			break
		}
	}
}

func testStringList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{"indexed field value a", "id0"},
		[]interface{}{"indexed field value b", "id1"},
		[]interface{}{"indexed field value c", "id2"},
		[]interface{}{"indexed field value d", "id3"},
		[]interface{}{"indexed field value e", "id4"},
		[]interface{}{"indexed field value f", "id5"},
		[]interface{}{"indexed field value g", "id6"},
		[]interface{}{"indexed field value h", "id7"},
		[]interface{}{"indexed field value i", "id8"},
		[]interface{}{"indexed field value j", "id9"},
		[]interface{}{"indexed field value k", "id10"},
		[]interface{}{"indexed field value l", "id11"},
		[]interface{}{"indexed field value m", "id12"},
		[]interface{}{"indexed field value n", "id13"},
		[]interface{}{"indexed field value o", "id14"},
		[]interface{}{"indexed field value p", "id15"},
		[]interface{}{"indexed field value q", "id16"},
		[]interface{}{"indexed field value r", "id17"},
		[]interface{}{"indexed field value s", "id18"},
		[]interface{}{"indexed field value t", "id19"},
		[]interface{}{"indexed field value u", "id20"},
		[]interface{}{"indexed field value v", "id21"},
		[]interface{}{"indexed field value w", "id22"},
		[]interface{}{"indexed field value x", "id23"},
		[]interface{}{"indexed field value y", "id24"},
		[]interface{}{"indexed field value z", "id25"},
	}
}

func testSameValueStringList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{"multiple IDs indexed", "id100"},
		[]interface{}{"multiple IDs indexed", "id110"},
		[]interface{}{"multiple IDs indexed", "id120"},
		[]interface{}{"multiple IDs indexed", "id130"},
		[]interface{}{"multiple IDs indexed", "id140"},
		[]interface{}{"multiple IDs indexed", "id150"},
		[]interface{}{"multiple IDs indexed", "id160"},
		[]interface{}{"multiple IDs indexed", "id170"},
		[]interface{}{"multiple IDs indexed", "id180"},
		[]interface{}{"multiple IDs indexed", "id190"},
	}
}

func testIntList() [][]interface{} {
	return [][]interface{}{
		[]interface{}{1, "id1"},
		[]interface{}{2, "id2"},
	}
}

// func TestStringQuery(t *testing.T) {
// 	selector := []string{"Add", "Street", "Name"}
// 	db, err := bolt.Open(internalTesting.Path, vars.FilePermission, nil)
// 	if err != nil {
// 		t.Error(err)
// 		return
// 	}
// 	defer os.Remove(db.Path())
//
// 	db.Update(func(tx *bolt.Tx) error {
// 		tx.CreateBucket(vars.InternalBuckectCollections)
// 		return nil
// 	})
//
// 	// i := index.NewStringIndex(internalTesting.Path, selector)
// 	col := NewCollection(db, "streetName")
// 	col.SetIndex("streetName", utils.StringComparatorType, selector)
//
// 	for _, val := range internalTesting.GetCompleteUsersExampleStreetNamesOnly() {
// 		user := val.(*internalTesting.CompleteUser)
// 		col.Put(user.GetID(), user.GetContent())
// 	}
//
// 	buildTestQuery := func(limit int, reverted bool, getActions, keepActions []*Action) *Query {
// 		if limit == 0 {
// 			limit = 1
// 		}
// 		q := NewQuery().SetLimit(limit)
// 		if reverted {
// 			q.InvertOrder()
// 		}
// 		if getActions != nil {
// 			for _, action := range getActions {
// 				action.Selector = selector
// 			}
// 		}
// 		q.GetActions = getActions
// 		if keepActions != nil {
// 			for _, action := range keepActions {
// 				action.Selector = selector
// 			}
// 		}
// 		q.KeepActions = keepActions
// 		return q
// 	}
//
// 	listOfTests := []struct {
// 		name           string
// 		query          *Query
// 		expectedResult []string
// 	}{
// 		{
// 			name: "Get Equal with limit 1",
// 			query: buildTestQuery(1, false,
// 				[]*Action{NewAction(Equal).CompareTo("North street")},
// 				nil),
// 			expectedResult: []string{"S_North_1"},
// 		}, {
// 			name: "Get Equal with limit 5",
// 			query: buildTestQuery(5, false,
// 				[]*Action{NewAction(Equal).CompareTo("North street")},
// 				nil),
// 			expectedResult: []string{"S_North_1", "S_North_6", "S_North_11", "S_North_16", "S_North_21"},
// 		}, {
// 			name: "Get Equal with limit 5 and reverted",
// 			query: buildTestQuery(5, true,
// 				[]*Action{NewAction(Equal).CompareTo("North street")},
// 				nil),
// 			expectedResult: []string{"S_North_21", "S_North_16", "S_North_11", "S_North_6", "S_North_1"},
// 		}, {
// 			name: "Get Greater and Equal - limit 15",
// 			query: buildTestQuery(15, false,
// 				[]*Action{NewAction(Greater).CompareTo("East street")},
// 				nil),
// 			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49", "S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25"},
// 		}, {
// 			name: "Get Greater - limit 10",
// 			query: buildTestQuery(10, false,
// 				[]*Action{NewAction(Greater).CompareTo("East street").EqualWanted()},
// 				nil),
// 			expectedResult: []string{"S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25", "S_George_30", "S_George_35", "S_George_40", "S_George_45", "S_North_1"},
// 		}, {
// 			name: "Get Less and Equal - limit 15",
// 			query: buildTestQuery(15, false,
// 				[]*Action{NewAction(Less).CompareTo("West street")},
// 				nil),
// 			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23", "S_West_28", "S_West_33", "S_West_38", "S_West_43", "S_West_48", "S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22"},
// 		}, {
// 			name: "Get Less - limit 10",
// 			query: buildTestQuery(10, false,
// 				[]*Action{NewAction(Less).CompareTo("West street").EqualWanted()},
// 				nil),
// 			expectedResult: []string{"S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22", "S_South_27", "S_South_32", "S_South_37", "S_South_42", "S_South_47"},
// 		}, {
// 			name:           "Empty Action",
// 			query:          buildTestQuery(10, false, nil, nil),
// 			expectedResult: []string{},
// 		}, {
// 			name: "Greater than last",
// 			query: buildTestQuery(5, false,
// 				[]*Action{NewAction(Greater).CompareTo("Z")},
// 				nil),
// 			expectedResult: []string{},
// 		}, {
// 			name: "Less than first",
// 			query: buildTestQuery(5, false,
// 				[]*Action{NewAction(Less).CompareTo("A")},
// 				nil),
// 			expectedResult: []string{},
// 		}, {
// 			name: "Greater from start",
// 			query: buildTestQuery(5, false,
// 				[]*Action{NewAction(Greater).CompareTo("A")},
// 				nil),
// 			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24"},
// 		}, {
// 			name: "Less from end",
// 			query: buildTestQuery(5, false,
// 				[]*Action{NewAction(Less).CompareTo("Z")},
// 				nil),
// 			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23"},
// 		}, {
// 			name: "Greater from start and keep after E - limit 20",
// 			query: buildTestQuery(100, false,
// 				[]*Action{NewAction(Greater).CompareTo("A")},
// 				[]*Action{NewAction(Greater).CompareTo("F")}),
// 			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49"},
// 		}, {
// 			name: "Duplicated",
// 			query: buildTestQuery(10, false,
// 				[]*Action{NewAction(Equal).CompareTo("North street Dup")},
// 				[]*Action{NewAction(Greater).CompareTo("North street Dup4")}).DistinctWanted(),
// 			expectedResult: []string{"DUP_1"},
// 		},
// 	}
//
// 	for _, test := range listOfTests {
// 		ids := col.Query(test.query)
// 		if !reflect.DeepEqual(test.expectedResult, ids) {
// 			if len(test.expectedResult) == 0 && len(ids) == 0 {
// 				continue
// 			}
// 			t.Errorf("%q the expected result is %v but had %v", test.name, test.expectedResult, ids)
// 			break
// 		}
// 	}
// }
//
// func testStringList() [][]interface{} {
// 	return [][]interface{}{
// 		[]interface{}{"indexed field value a", "id0"},
// 		[]interface{}{"indexed field value b", "id1"},
// 		[]interface{}{"indexed field value c", "id2"},
// 		[]interface{}{"indexed field value d", "id3"},
// 		[]interface{}{"indexed field value e", "id4"},
// 		[]interface{}{"indexed field value f", "id5"},
// 		[]interface{}{"indexed field value g", "id6"},
// 		[]interface{}{"indexed field value h", "id7"},
// 		[]interface{}{"indexed field value i", "id8"},
// 		[]interface{}{"indexed field value j", "id9"},
// 		[]interface{}{"indexed field value k", "id10"},
// 		[]interface{}{"indexed field value l", "id11"},
// 		[]interface{}{"indexed field value m", "id12"},
// 		[]interface{}{"indexed field value n", "id13"},
// 		[]interface{}{"indexed field value o", "id14"},
// 		[]interface{}{"indexed field value p", "id15"},
// 		[]interface{}{"indexed field value q", "id16"},
// 		[]interface{}{"indexed field value r", "id17"},
// 		[]interface{}{"indexed field value s", "id18"},
// 		[]interface{}{"indexed field value t", "id19"},
// 		[]interface{}{"indexed field value u", "id20"},
// 		[]interface{}{"indexed field value v", "id21"},
// 		[]interface{}{"indexed field value w", "id22"},
// 		[]interface{}{"indexed field value x", "id23"},
// 		[]interface{}{"indexed field value y", "id24"},
// 		[]interface{}{"indexed field value z", "id25"},
// 	}
// }
//
// func testSameValueStringList() [][]interface{} {
// 	return [][]interface{}{
// 		[]interface{}{"multiple IDs indexed", "id100"},
// 		[]interface{}{"multiple IDs indexed", "id110"},
// 		[]interface{}{"multiple IDs indexed", "id120"},
// 		[]interface{}{"multiple IDs indexed", "id130"},
// 		[]interface{}{"multiple IDs indexed", "id140"},
// 		[]interface{}{"multiple IDs indexed", "id150"},
// 		[]interface{}{"multiple IDs indexed", "id160"},
// 		[]interface{}{"multiple IDs indexed", "id170"},
// 		[]interface{}{"multiple IDs indexed", "id180"},
// 		[]interface{}{"multiple IDs indexed", "id190"},
// 	}
// }
//
// func testIntList() [][]interface{} {
// 	return [][]interface{}{
// 		[]interface{}{1, "id1"},
// 		[]interface{}{2, "id2"},
// 	}
// }
