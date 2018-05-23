package query

// Defines the different types of queries:
const (
	Equal   ActionType = "eq"
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
		Distinct  bool
	}

	// Action defines the way the query will be performed
	Action struct {
		Operation      ActionType
		CompareToValue interface{}
	}

	// ActionType defines the type of action to perform.
	ActionType string
)

// NewAction returns a new Action pointer with the given ActionType
func NewAction(t ActionType) *Action {
	return &Action{
		Operation: t,
	}
}

// CompareTo defines the value you want to compare to
func (a *Action) CompareTo(val interface{}) *Action {
	a.CompareToValue = val
	return a
}

// GetType returns the type of the action given at the initialisation
func (a *Action) GetType() ActionType {
	return a.Operation
}

// NewQuery build a new query object with the given selector.
// It also set the default limit to 1000.
func NewQuery(selector []string) *Query {
	return &Query{
		Selector: selector,
		Limit:    1000,
	}
}

// SetLimit defines the configurable limit of IDs.
func (q *Query) SetLimit(l int) *Query {
	q.Limit = l
	return q
}

// InvertOrder lets the caller invert the slice if wanted.
func (q *Query) InvertOrder() *Query {
	q.InvertedOrder = true
	return q
}

// DistinctWanted clean the duplicated IDs
func (q *Query) DistinctWanted() *Query {
	q.Distinct = true
	return q
}

// EqualWanted defines if the exact corresponding key is retrieved or not.
func (q *Query) EqualWanted() *Query {
	q.KeepEqual = true
	return q
}

// Get defines the action to perform to get IDs
func (q *Query) Get(a *Action) *Query {
	q.GetAction = a
	return q
}

// Keep defines the action to perform to clean IDs which have already retrieved
// by the Get action.
func (q *Query) Keep(a *Action) *Query {
	q.KeepAction = a
	return q
}
