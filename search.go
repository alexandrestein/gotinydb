package gotinydb

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
)

type (
	// SearchResult is returned bu *Collection.Search or *Collection.SearchWithOptions.
	// It provides a easy listing of the result.
	SearchResult struct {
		BleveSearchResult *bleve.SearchResult

		position int
		c        *Collection
	}

	// Response are returned by *SearchResult.NextResponse if the caller needs to
	// have access to the byte stream
	Response struct {
		ID            string
		Content       []byte
		DocumentMatch *search.DocumentMatch
	}
)

// Next fills up the destination by marshaling the saved byte stream.
// It returns an error if any and the coresponding id of the element.
func (s *SearchResult) Next(dest interface{}) (id string, err error) {
	id, _, _, err = s.next(dest)
	return id, err
}

// NextResponse fills up the destination by marshaling the saved byte stream.
// It returns the byte stream and the id of the document inside a Response pointer or an error if any.
func (s *SearchResult) NextResponse(dest interface{}) (resp *Response, _ error) {
	resp = new(Response)
	id, content, docMatch, err := s.next(dest)
	if err != nil {
		return nil, err
	}

	resp.ID = id
	resp.Content = content
	resp.DocumentMatch = docMatch
	return resp, err
}

func (s *SearchResult) next(dest interface{}) (id string, content []byte, docMatch *search.DocumentMatch, err error) {
	if s.position >= s.BleveSearchResult.Hits.Len() {
		return "", nil, nil, ErrEndOfQueryResult
	}

	docMatch = s.BleveSearchResult.Hits[s.position]
	id = docMatch.ID
	content, err = s.c.Get(id, dest)

	s.position++

	return
}
