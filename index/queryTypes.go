package index

// Defines the different types of queries
const (
	Equal     QueryType = "eq"
	StartWith QueryType = "sw"
)

type (
	// Query defines the object to request index query.
	Query struct {
		index *Index

		PathToFiled []string
		Action      QueryType
		Limit       int
	}

	// QueryType defines the type of query the caller needs to do.
	QueryType string
)

// Query permits to chain queries
func (q *Query) Query(next *Query) {

}
