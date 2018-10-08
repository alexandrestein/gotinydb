package gotinydb

import (
	"testing"

	"github.com/blevesearch/bleve"
)

func TestIndexExistingValue(t *testing.T) {
	defer clean()
	open(t)

	complexObjectID := "complex object ID"
	complexObject := &struct {
		Name string
		Car  struct {
			Brand   string
			Value   int
			Options []string
		}
	}{
		"Ugo",
		struct {
			Brand   string
			Value   int
			Options []string
		}{
			"BMW",
			10000,
			[]string{"cruse", "esp"},
		},
	}

	err := testCol.Put(complexObjectID, complexObject)
	if err != nil {
		t.Error(err)
		return
	}

	err = testCol.SetBleveIndex("car brand", bleve.NewIndexMapping())
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.SetBleveIndex("car brand", bleve.NewIndexMapping())
	if err == nil {
		t.Error("setting index with same name must returns an error")
		return
	}

	query := bleve.NewQueryStringQuery("BMW")
	searchRequest := bleve.NewSearchRequestOptions(query, 10, 0, true)
	var searchResult *SearchResult
	searchResult, err = testCol.Search("car brand", searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log(searchResult.BleveSearchResult)
	}
}
