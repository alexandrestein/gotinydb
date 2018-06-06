package gotinydb

// Thoses constants defines the different types of action to perform at query
const (
	Equal   ActionType = "eq"
	Greater ActionType = "gr"
	Less    ActionType = "le"
)

type (
	// Query defines the object to request index query.
	Query struct {
		GetActions, CleanActions []*Action

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
		Equal          bool
	}

	// ActionType defines the type of action to perform.
	ActionType string
)

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
		q.GetActions = []*Action{}
	}
	q.GetActions = append(q.GetActions, a)
	return q
}

// Clean defines the actions to perform to clean IDs which have already retrieved
// by the Get actions.
func (q *Query) Clean(a *Action) *Query {
	if q.CleanActions == nil {
		q.GetActions = []*Action{a}
	}
	q.CleanActions = append(q.CleanActions, a)
	return q
}
