package gotinydb

import "github.com/blevesearch/bleve"

type (
	SearchResult struct {
		BleveSearchResult *bleve.SearchResult

		position int
		c        *Collection
	}
)

func (s *SearchResult) Next(dest interface{}) ([]byte, error) {
	if s.position >= s.BleveSearchResult.Hits.Len() {
		return nil, ErrEndOfQueryResult
	}

	content, err := s.c.Get(s.BleveSearchResult.Hits[s.position].ID, dest)

	s.position++

	return content, err
}
