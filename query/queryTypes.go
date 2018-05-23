package query

// Defines the different types of queries:
const (
	// notSet ActionType = ""

	Equal ActionType = "eq"
	// NotEqual ActionType = "nq"
	Greater ActionType = "gr"
	Less    ActionType = "le"
)

type (
	// Query defines the object to request index query.
	Query struct {
		Selector              []string
		GetAction, KeepAction *Action
		// Actions       map[ActionType]*Action
		InvertedOrder bool
		Limit         int

		KeepEqual bool

		// // Result is the respond of IDs
		// Result []string
	}

	Action struct {
		Operation      ActionType
		CompareToValue interface{}
	}

	// ActionType defines the type of action to perform.
	ActionType string
)

func NewAction(t ActionType) *Action {
	return &Action{
		Operation: t,
	}
}

func (a *Action) CompareTo(val interface{}) *Action {
	a.CompareToValue = val
	return a
}
func (a *Action) SetType(op ActionType) *Action {
	a.Operation = op
	return a
}
func (a *Action) GetType() ActionType {
	return a.Operation
}

// func (a *Action) Valid() bool {
// 	if a.Operation == notSet {
// 		return false
// 	}
// 	if a.CompareToValue == nil {
// 		return false
// 	}
// 	return true
// }

func NewQuery(selector []string) *Query {
	return &Query{
		Selector: selector,
		Limit:    1,
	}
}

func (q *Query) SetLimit(l int) *Query {
	q.Limit = l
	return q
}

func (q *Query) InvertOrder() *Query {
	q.InvertedOrder = true
	return q
}

func (q *Query) EqualWanted() *Query {
	q.KeepEqual = true
	return q
}

func (q *Query) Get(a *Action) *Query {
	q.GetAction = a
	return q
}

func (q *Query) Keep(a *Action) *Query {
	q.KeepAction = a
	return q
}
