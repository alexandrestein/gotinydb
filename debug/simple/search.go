package simple

import "github.com/blevesearch/bleve"

type (
	SearchResult struct {
		BleveSearchResult *bleve.SearchResult

		position uint64
		c        *Collection
	}
)

