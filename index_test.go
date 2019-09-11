package gotinydb

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve"
)

func TestIndexExistingValue(t *testing.T) {
	defer clean()
	openT(t)

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

	docMapping := bleve.NewDocumentMapping()
	// carMapping := bleve.NewDocumentStaticMapping()
	// fieldMapping := bleve.NewTextFieldMapping()
	// fieldMapping.Analyzer = simple.Name
	// carMapping.AddFieldMappingsAt("Brand", fieldMapping)
	// docMapping.AddSubDocumentMapping("Car", carMapping)

	err = testCol.SetBleveIndex("car brand", docMapping)
	if err != nil {
		t.Error(err)
		return
	}
	err = testCol.SetBleveIndex("car brand", docMapping)
	if err == nil {
		t.Error("setting index with same name must returns an error")
		return
	}

	query := bleve.NewMatchQuery("BMW")
	searchRequest := bleve.NewSearchRequestOptions(query, 10, 0, true)

	var searchResult *SearchResult
	searchResult, err = testCol.SearchWithOptions("car brand", searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	if testing.Verbose() {
		t.Log(searchResult.BleveSearchResult)
	}
}

func TestIndexResultNext(t *testing.T) {
	defer clean()
	openT(t)

	testCol.Delete(testUserID)

	userDocumentMapping := bleve.NewDocumentStaticMapping()

	emailFieldMapping := bleve.NewTextFieldMapping()
	userDocumentMapping.AddFieldMappingsAt("email", emailFieldMapping)

	nameFieldMapping := bleve.NewTextFieldMapping()
	userDocumentMapping.AddFieldMappingsAt("name", nameFieldMapping)

	accountDocumentMapping := bleve.NewDocumentMapping()
	userDocumentMapping.AddSubDocumentMapping("oauth", accountDocumentMapping)

	err := testCol.SetBleveIndex("test index name", userDocumentMapping)
	if err != nil {
		t.Error(err)
		return
	}

	user1 := &testUserStruct{
		"ali", "ali-kan@gmail.com", &Account{"Google", "https://google.com"},
	}
	user2 := &testUserStruct{
		"bernard", "beber@gmail.com", &Account{"Google", "https://google.com"},
	}
	user3 := &testUserStruct{
		"george", "gg@aol.com", &Account{"GitHub", "https://github.com"},
	}

	testCol.Put("user1", user1)
	testCol.Put("user2", user2)
	testCol.Put("user3", user3)

	query := bleve.NewWildcardQuery("*gmail*")
	searchRequest := bleve.NewSearchRequestOptions(query, 10, 0, true)
	searchRequest.SortBy([]string{"_id"})
	var searchResult *SearchResult
	searchResult, err = testCol.SearchWithOptions("test index name", searchRequest)
	if err != nil {
		t.Error(err)
		return
	}

	retrievedUser := new(testUserStruct)
	_, err = searchResult.Next(retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(retrievedUser, user1) {
		t.Errorf("unexpected response")
		return
	}
	retrievedUser = new(testUserStruct)
	_, err = searchResult.Next(retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(retrievedUser, user2) {
		t.Errorf("unexpected response")
		return
	}

	if testing.Verbose() {
		t.Log(searchResult)
	}

	query = bleve.NewWildcardQuery("*aol*")
	query.SetField("email")
	searchResult, err = testCol.Search("test index name", query)
	if err != nil {
		t.Error(err)
		return
	}

	retrievedUser = new(testUserStruct)
	_, err = searchResult.Next(retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(retrievedUser, user3) {
		t.Errorf("unexpected response")
		return
	}

	if testing.Verbose() {
		t.Log(searchResult)
	}

	query2 := bleve.NewMatchQuery("george")
	searchResult, err = testCol.Search("test index name", query2)
	if err != nil {
		t.Error(err)
		return
	}

	retrievedUser = new(testUserStruct)
	_, err = searchResult.NextResponse(retrievedUser)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(retrievedUser, user3) {
		t.Errorf("unexpected response")
		return
	}

	if testing.Verbose() {
		t.Log(searchResult)
	}

	// Do next twice to pass the clone
	_, err = searchResult.Next(retrievedUser)
	_, err = searchResult.Next(retrievedUser)
	if err == nil {
		t.Errorf("there is no more result and this must returns an error")
		return
	}
}
