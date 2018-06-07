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
		getActions, cleanActions []*Action

		orderBy []string
		// Increasing bool

		limit int

		distinct bool
	}

	// Action defines the way the query will be performed
	Action struct {
		selector       []string
		operation      ActionType
		compareToValue interface{}
		equal          bool

		limit int
	}

	// ActionType defines the type of action to perform.
	ActionType string
)

// NewQuery build a new query object.
// It also set the default limit to 1000.
func NewQuery() *Query {
	return &Query{
		limit: 1000,
	}
}

// SetLimit defines the configurable limit of IDs.
func (q *Query) SetLimit(l int) *Query {
	q.limit = l
	for _, action := range q.getActions {
		action.limit = l
	}
	for _, action := range q.cleanActions {
		action.limit = l
	}
	return q
}

// // InvertOrder lets the caller invert the slice if wanted.
// func (q *Query) InvertOrder() *Query {
// 	q.InvertedOrder = true
// 	return q
// }

// DistinctWanted clean the duplicated IDs
func (q *Query) DistinctWanted() *Query {
	q.distinct = true
	return q
}

// Get defines the action to perform to get IDs
func (q *Query) Get(a *Action) *Query {
	if q.getActions == nil {
		q.getActions = []*Action{}
	}
	a.limit = q.limit
	q.getActions = append(q.getActions, a)
	return q
}

// Clean defines the actions to perform to clean IDs which have already retrieved
// by the Get actions.
func (q *Query) Clean(a *Action) *Query {
	if q.cleanActions == nil {
		q.getActions = []*Action{a}
	}
	a.limit = q.limit
	q.cleanActions = append(q.cleanActions, a)
	return q
}
