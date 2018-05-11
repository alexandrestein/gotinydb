package index

const (
	Equal     QueryType = "eq"
	StartWith QueryType = "sw"
)

type (
	Query struct {
		index *Index

		PathToFiled []string
		Action      QueryType
		Limit       int
	}

	QueryType string
)

// Query permits to chain queries
func (q *Query) Query(next *Query) {

}
