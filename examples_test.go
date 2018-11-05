package gotinydb_test

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/alexandrestein/gotinydb"
	"github.com/blevesearch/bleve"
)

type (
	user struct {
		ID        int
		Email     string
		LastLogin time.Time
	}
)

var (
	testTime time.Time

	dbPath string
	dbKey  [32]byte

	exampleDB         *gotinydb.DB
	exampleCollection *gotinydb.Collection
)

func init() {
	testTime.UnmarshalText([]byte("2018-11-05T12:20:44.588809926+01:00"))

	dbPath = os.TempDir() + "/example"
	dbKey = [32]byte{}

	os.RemoveAll(dbPath)
	os.RemoveAll(os.TempDir() + "/package_example")
	os.RemoveAll("path_to_database_directory")

	var err error
	// Open or create the database at the given path and with the given encryption key
	exampleDB, err = gotinydb.Open(dbPath, dbKey)
	if err != nil {
		log.Fatal(err)
	}

	// Open a collection
	exampleCollection, err = exampleDB.Use("users")
	if err != nil {
		log.Fatal(err)
	}

	userDocumentMapping := bleve.NewDocumentMapping()
	indexMapping := bleve.NewIndexMapping()
	// This is because you can have multiple mapping for the same index
	indexMapping.AddDocumentMapping("main", userDocumentMapping)
	exampleCollection.SetBleveIndex("index X", indexMapping)
	doc := &struct {
		Name string
	}{
		"I'm the example document",
	}
	exampleCollection.Put("index X document", doc)

	var writer gotinydb.Writer
	writer, err = exampleDB.GetFileWriter("read file", "txt")
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()
	writer.Write([]byte("JbBSFroiVAtjQy6bR3xXgrPY2GFvOPRqvWxfHUmAFAksELPTpV0lmPvwjMwdqq5i"))
}

func Example() {
	// Open or create the database at the given path and with the given encryption key
	db, err := gotinydb.Open(os.TempDir()+"/package_example", dbKey)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Open a collection
	var c *gotinydb.Collection
	c, err = db.Use("users")
	if err != nil {
		log.Fatal(err)
	}

	// Build the index mapping (take a look at bleve)
	// This is a static mapping document to index only specified fields
	userDocumentMapping := bleve.NewDocumentStaticMapping()
	// Build the field checker
	emailFieldMapping := bleve.NewTextFieldMapping()
	// Add a text filed to Email property
	userDocumentMapping.AddFieldMappingsAt("Email", emailFieldMapping)
	// Build the index mapping it self
	indexMapping := bleve.NewIndexMapping()
	// This is because you can have multiple mapping for the same index
	indexMapping.AddDocumentMapping("exampleUser", userDocumentMapping)

	// In this case it indexes only the field "Email"

	// Save the bleve indexexes
	err = c.SetBleveIndex("email", indexMapping)
	if err != nil {
		if err != gotinydb.ErrNameAllreadyExists {
			log.Fatal(err)
		}
	}

	// Example user
	record := &user{
		316,
		"jonas-90@tlaloc.com",
		testTime,
	}

	// Save it in DB
	if err = c.Put("id", record); err != nil {
		log.Fatal(err)
	}

	// Build the query
	query := bleve.NewQueryStringQuery(record.Email)
	// Add the query to the search
	var searchResult *gotinydb.SearchResult
	searchResult, err = c.Search("email", query)
	if err != nil {
		log.Fatal(err)
	}

	// Convert the reccored into a struct using JSON internally
	retrievedRecord := new(user)

	id, respErr := searchResult.Next(retrievedRecord)
	if respErr != nil {
		log.Fatal(respErr)
	}

	// Display the result
	fmt.Println(id)
	fmt.Println(retrievedRecord.ID, retrievedRecord.Email, retrievedRecord.LastLogin.Format(time.Stamp))

	// Output: id
	// 316 jonas-90@tlaloc.com Nov  5 12:20:44
}

func ExampleOpen() {
	// Open or create the database at the given path and with the given encryption key
	db, err := gotinydb.Open("path_to_database_directory", dbKey)
	if err != nil {
		log.Fatal(err)
	}

	// Remumber to close the database
	err = db.Close()

	fmt.Println(err)
	// Output: <nil>
}

func ExampleDB_Use() {
	// Open a collection
	col, err := exampleDB.Use("collection name")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(col.Name)
	fmt.Println(col.Prefix)
	// Output: collection name
	// [1 20 101]
}

func ExampleCollection_Put() {
	record := &struct{}{}

	err := exampleCollection.Put("id", record)
	fmt.Println(err)
	// Output: <nil>
}

func ExampleCollection_Get() {
	record := &struct {
		Name string
	}{
		"testing name",
	}

	exampleCollection.Put("id", record)

	retrievedRecord := &struct {
		Name string
	}{}

	recordAsBytes, _ := exampleCollection.Get("id", retrievedRecord)

	fmt.Println(string(recordAsBytes))

	fmt.Println(retrievedRecord)
	// Output: {"Name":"testing name"}
	// &{testing name}
}

func ExampleWriter() {
	writer, err := exampleDB.GetFileWriter("file example", "test.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	n := 0
	writtenBytes := 0
	n, err = writer.Write([]byte("this is a text file"))
	if err != nil {
		log.Fatal(err)
	}
	writtenBytes += n
	n, err = writer.Write([]byte("\n"))
	if err != nil {
		log.Fatal(err)
	}
	writtenBytes += n
	n, err = writer.Write([]byte("and then the second is written"))
	if err != nil {
		log.Fatal(err)
	}
	writtenBytes += n

	fmt.Println("writtenBytes", writtenBytes)

	readBuff := make([]byte, 1000)

	writer.Seek(0, io.SeekStart)

	n, err = writer.Read(readBuff)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	fmt.Println(n, string(readBuff[:n]))

	// Output: writtenBytes 50
	// 50 this is a text file
	// and then the second is written
}

func ExampleReader() {
	reader, err := exampleDB.GetFileReader("read file")
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	readBuffer := make([]byte, 100)
	n := 0
	n, err = reader.ReadAt(readBuffer, 25)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	readBuffer = readBuffer[:n]

	fmt.Println(n, string(readBuffer))

	// Output: 39 GFvOPRqvWxfHUmAFAksELPTpV0lmPvwjMwdqq5i
}

func ExampleCollection_SetBleveIndex() {
	// Build the index mapping (take a look at bleve)
	// This is a static mapping document to index only specified fields
	userDocumentMapping := bleve.NewDocumentStaticMapping()
	// Build the field checker
	emailFieldMapping := bleve.NewTextFieldMapping()
	// Add a text filed to Email property
	userDocumentMapping.AddFieldMappingsAt("Email", emailFieldMapping)
	// Build the index mapping it self
	indexMapping := bleve.NewIndexMapping()
	// This is because you can have multiple mapping for the same index
	indexMapping.AddDocumentMapping("exampleUser", userDocumentMapping)

	err := exampleCollection.SetBleveIndex("your index name", indexMapping)
	fmt.Println(err)

	// Output: <nil>
}

func ExampleCollection_Search() {
	query := bleve.NewMatchQuery("example")

	response, err := exampleCollection.Search("index X", query)
	if err != nil {
		log.Fatal(err)
	}

	dest := &struct{ Name string }{}
	var resp *gotinydb.Response
	resp, err = response.NextResponse(dest)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(dest)
	fmt.Println(resp.ID)
	fmt.Println(string(resp.Content))
	fmt.Println(resp.DocumentMatch.Score)

	// Output: &{I'm the example document}
	// index X document
	// {"Name":"I'm the example document"}
	// 0.7071067690849304
}
