package gotinydb

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
