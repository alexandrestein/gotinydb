package gotinydb

import "github.com/blevesearch/bleve"

type (
	SearchResult struct {
		BleveSearchResult *bleve.SearchResult

		position int
		c        *Collection
	}

	Response struct {
		ID      string
		Content []byte
	}
)

func (s *SearchResult) Next(dest interface{}) (id string, err error) {
	id, _, err = s.next(dest)
	return id, err
}

func (s *SearchResult) NextResponse(dest interface{}) (resp *Response, _ error) {
	resp = new(Response)
	id, content, err := s.next(dest)
	if err != nil {
		return nil, err
	}

	resp.ID = id
	resp.Content = content
	return resp, err
}

func (s *SearchResult) next(dest interface{}) (id string, content []byte, err error) {
	if s.position >= s.BleveSearchResult.Hits.Len() {
		return "", nil, ErrEndOfQueryResult
	}

	id = s.BleveSearchResult.Hits[s.position].ID
	content, err = s.c.Get(id, dest)

	s.position++

	return id, content, err
}
