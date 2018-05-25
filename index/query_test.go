package index

import (
	"os"
	"reflect"
	"testing"

	"gitea.interlab-net.com/alexandre/db/query"
	internalTesting "gitea.interlab-net.com/alexandre/db/testing"
)

func TestStringQuery(t *testing.T) {
	defer os.RemoveAll(internalTesting.Path)
	selector := []string{"Add", "Street", "Name"}
	i := NewString(internalTesting.Path+"/indexTest", selector)
	for _, val := range internalTesting.GetCompleteUsersExampleStreetNamesOnly() {
		user := val.(*internalTesting.CompleteUser)
		i.Put(user.Add.Street.Name, val.GetID())
	}

	buildTestQuery := func(limit int, reverted bool, getAction, keepAction *query.Action) *query.Query {
		if limit == 0 {
			limit = 1
		}
		q := query.NewQuery(selector).SetLimit(limit)
		if reverted {
			q.InvertOrder()
		}
		q.GetAction = getAction
		q.KeepAction = keepAction
		return q
	}

	listOfTests := []struct {
		name           string
		query          *query.Query
		expectedResult []string
	}{
		{
			name:           "Get Equal with limit 1",
			query:          buildTestQuery(1, false, query.NewAction(query.Equal).CompareTo("North street"), nil),
			expectedResult: []string{"S_North_1"},
		}, {
			name:           "Get Equal with limit 5",
			query:          buildTestQuery(5, false, query.NewAction(query.Equal).CompareTo("North street"), nil),
			expectedResult: []string{"S_North_1", "S_North_6", "S_North_11", "S_North_16", "S_North_21"},
		}, {
			name:           "Get Equal with limit 5 and reverted",
			query:          buildTestQuery(5, true, query.NewAction(query.Equal).CompareTo("North street"), nil),
			expectedResult: []string{"S_North_21", "S_North_16", "S_North_11", "S_North_6", "S_North_1"},
		}, {
			name:           "Get Greater and Equal - limit 15",
			query:          buildTestQuery(15, false, query.NewAction(query.Greater).CompareTo("East street"), nil),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49", "S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25"},
		}, {
			name:           "Get Greater - limit 10",
			query:          buildTestQuery(10, false, query.NewAction(query.Greater).CompareTo("East street"), nil).EqualWanted(),
			expectedResult: []string{"S_George_5", "S_George_10", "S_George_15", "S_George_20", "S_George_25", "S_George_30", "S_George_35", "S_George_40", "S_George_45", "S_North_21"},
		}, {
			name:           "Get Less and Equal - limit 15",
			query:          buildTestQuery(15, false, query.NewAction(query.Less).CompareTo("West street"), nil),
			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23", "S_West_28", "S_West_33", "S_West_38", "S_West_43", "S_West_48", "S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22"},
		}, {
			name:           "Get Less - limit 10",
			query:          buildTestQuery(10, false, query.NewAction(query.Less).CompareTo("West street"), nil).EqualWanted(),
			expectedResult: []string{"S_South_2", "S_South_7", "S_South_12", "S_South_17", "S_South_22", "S_South_27", "S_South_32", "S_South_37", "S_South_42", "S_South_47"},
		}, {
			name:           "Empty Action",
			query:          buildTestQuery(10, false, nil, nil),
			expectedResult: []string{},
		}, {
			name:           "Greater than last",
			query:          buildTestQuery(5, false, query.NewAction(query.Greater).CompareTo("Z"), nil),
			expectedResult: []string{},
		}, {
			name:           "Less than first",
			query:          buildTestQuery(5, false, query.NewAction(query.Less).CompareTo("A"), nil),
			expectedResult: []string{},
		}, {
			name:           "Greater from start",
			query:          buildTestQuery(5, false, query.NewAction(query.Greater).CompareTo("A"), nil),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24"},
		}, {
			name:           "Less from end",
			query:          buildTestQuery(5, false, query.NewAction(query.Less).CompareTo("Z"), nil),
			expectedResult: []string{"S_West_3", "S_West_8", "S_West_13", "S_West_18", "S_West_23"},
		}, {
			name:           "Greater from start and keep after E - limit 20",
			query:          buildTestQuery(100, false, query.NewAction(query.Greater).CompareTo("A"), query.NewAction(query.Greater).CompareTo("F")),
			expectedResult: []string{"S_East_4", "S_East_9", "S_East_14", "S_East_19", "S_East_24", "S_East_29", "S_East_34", "S_East_39", "S_East_44", "S_East_49"},
		}, {
			name:           "Duplicated",
			query:          buildTestQuery(10, false, query.NewAction(query.Equal).CompareTo("North street Dup"), query.NewAction(query.Greater).CompareTo("North street Dup4")).DistinctWanted(),
			expectedResult: []string{"DUP_1"},
		},
	}
	for _, test := range listOfTests {
		ids := i.RunQuery(test.query)
		if !reflect.DeepEqual(test.expectedResult, ids) {
			if len(test.expectedResult) == 0 && len(ids) == 0 {
				continue
			}
			t.Errorf("%q the expected result is %v but had %v", test.name, test.expectedResult, ids)
		}
	}
	i.getTree().Clear()
}
