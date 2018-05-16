package index

// Defines the different types of queries:
const (
	Equal     QueryType = "eq"
	StartWith QueryType = "sw"
)

type (
	// Query defines the object to request index query.
	Query struct {
		index *Index
		next  *Query

		filedPath     []string
		Action        QueryType
		InvertedOrder bool
		Limit         int
	}

	// QueryType defines the type of query the caller needs to do.
	QueryType string
)

// Next permits to chain queries
func (q *Query) Next() (next *Query) {
	if q.next != nil {
		return q.next
	}

	q.next = &Query{
		index: q.index,
	}

	return q.next
}
