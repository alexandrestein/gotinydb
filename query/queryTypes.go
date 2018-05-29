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
		GetActions, KeepActions []*Action

		OrderBy       []string
		InvertedOrder bool

		Limit int

		Distinct bool
	}

	// Action defines the way the query will be performed
	Action struct {
		Selector       []string
		Operation      ActionType
		CompareToValue interface{}
		KeepEqual      bool
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

// EqualWanted defines if the exact corresponding key is retrieved or not.
func (a *Action) EqualWanted() *Action {
	a.KeepEqual = true
	return a
}

// NewQuery build a new query object.
// It also set the default limit to 1000.
func NewQuery() *Query {
	return &Query{
		Limit: 1000,
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

// Get defines the action to perform to get IDs
func (q *Query) Get(a *Action) *Query {
	if q.GetActions == nil {
		q.GetActions = []*Action{a}
	}
	q.GetActions = append(q.GetActions, a)
	return q
}

// Keep defines the action to perform to clean IDs which have already retrieved
// by the Get action.
func (q *Query) Keep(a *Action) *Query {
	if q.KeepActions == nil {
		q.GetActions = []*Action{a}
	}
	q.KeepActions = append(q.KeepActions, a)
	return q
}
