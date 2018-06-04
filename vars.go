package gotinydb

const (
	// TreeOrder defines the index tree
	TreeOrder = 10

	// FilePermission defines the database file permission
	FilePermission = 0740 // u -> rwx | g -> r-- | o -> ---
)

// Thoses constants defines the different types of action to perform at query
const (
	Equal   ActionType = "eq"
	Greater ActionType = "gr"
	Less    ActionType = "le"
)

var (
	meta  bucketName = "meta"
	data  bucketName = "data"
	refs  bucketName = "refs"
	index bucketName = "index"
)

var (
	StringIndexType IndexType = iota
	IntIndexType    IndexType
	UintIndexType   IndexType
	Foat32IndexType IndexType
	Foat64IndexType IndexType
	TimeIndexType   IndexType
)

// Internal common buckets and IDs
var (
// Root buckets
// The database is design like this:
// <b>  | root
// <b>  |-_metas
// <o>  | | {_collections: [collectionObject, collectionObject]}
// <b>  | |-collectionName1
// <l>  | | |[refID1, refID2, refID3...]
// <b>  | |-collectionName2
// <l>  | | |[refID1, refID2, refID3...]
// <b>  |-_collections
// <b>    |-collectionName1
// <l>    | |[ID1, ID2, ID3...]
// <b>    |-collectionName2
// <l>    | |[ID1, ID2, ID3...]
// internalBuckectMetaDatas   = []byte("_metas")
// internalBuckectCollections = []byte("_collections")

// getInternalMetaBucket = func(tx *bolt.Tx) *bolt.Bucket {
// 	// Create the bucket if not exist
// 	b := tx.Bucket(internalBuckectMetaDatas)
// 	if b == nil {
// 		b, _ = tx.CreateBucketIfNotExists(internalBuckectMetaDatas)
// 	}
// 	return b
// }
// getInternalCollectionsBucket = func(tx *bolt.Tx) *bolt.Bucket {
// 	// Create the bucket if not exist
// 	b := tx.Bucket(internalBuckectCollections)
// 	if b == nil {
// 		b, _ = tx.CreateBucketIfNotExists(internalBuckectCollections)
// 	}
// 	return b
// }
// saveCollection = func(tx *bolt.Tx, col *Collection) error {
// 	cols := getCollections(tx)
// 	cols[col.Name] = col
//
// 	colsAsBytes, marshalErr := json.Marshal(cols)
// 	if marshalErr != nil {
// 		return fmt.Errorf("marshaling collections map: %s", marshalErr.Error())
// 	}
//
// 	if putErr := getInternalMetaBucket(tx).Put(internalBuckectCollections, colsAsBytes); putErr != nil {
// 		return fmt.Errorf("inserting collections map into DB: %s", putErr.Error())
// 	}
// 	return nil
// }
// getCollections = func(tx *bolt.Tx) map[string]*Collection {
// 	collectionsMapAsBytes := getInternalMetaBucket(tx).Get(internalBuckectCollections)
// 	cols := map[string]*Collection{}
// 	// If the response is empty return a empty list
// 	if len(collectionsMapAsBytes) == 0 {
// 		return cols
// 	}
//
// 	// Convert the json into map listing collections
// 	if err := json.Unmarshal(collectionsMapAsBytes, &cols); err != nil {
// 		return nil
// 	}
// 	// Returns the slice of names
// 	return cols
// }

// getCollectionsNames = func(tx *bolt.Tx) []string {
// 	// Gets the collections name from the meta data collection bucket and the
// 	// internalBuckectCollections id object.
// 	namesAsBytes := getInternalMetaBucket(tx).Get(internalBuckectCollections)
// 	var names []string
// 	// If the response is empty return a empty list
// 	if len(namesAsBytes) == 0 {
// 		return names
// 	}
// 	// Convert the json into slice of names as strings
// 	if err := json.Unmarshal(namesAsBytes, ids); err != nil {
// 		return nil
// 	}
// 	// Returns the slice of names
// 	return names
// }

// 	getCollectionBucket = func(tx *bolt.Tx, colName string) *bolt.Bucket {
// 		// Create the bucket if not exist
// 		internalBucket := getInternalCollectionsBucket(tx)
// 		b := internalBucket.Bucket([]byte(colName))
// 		if b == nil {
// 			b, _ = internalBucket.CreateBucketIfNotExists([]byte(colName))
// 		}
// 		return b
// 	}
// 	getCollectionMetaBucket = func(tx *bolt.Tx, colName string) *bolt.Bucket {
// 		// Create the bucket if not exist
// 		internalBucket := getInternalMetaBucket(tx)
// 		b := internalBucket.Bucket([]byte(colName))
// 		if b == nil {
// 			b, _ = internalBucket.CreateBucketIfNotExists([]byte(colName))
// 		}
// 		return b
// 	}
// 	setObjectReferences = func(tx *bolt.Tx, colName, id string, refs []*IndexReference) error {
// 		// Built JSON
// 		refsAsBytes, marshalErr := json.Marshal(refs)
// 		if marshalErr != nil {
// 			return fmt.Errorf("marshaling object references: %s", marshalErr.Error())
// 		}
//
// 		// Saves the references into database
// 		b := getCollectionMetaBucket(tx, colName)
// 		if putErr := b.Put([]byte(id), refsAsBytes); putErr != nil {
// 			return fmt.Errorf("inserting reference into DB: %s", putErr.Error())
// 		}
// 		return nil
// 	}
// 	getObjectReferences = func(tx *bolt.Tx, colName, id string) []*IndexReference {
// 		b := getCollectionMetaBucket(tx, colName)
// 		if b == nil {
// 			return nil
// 		}
// 		// Get the reference of the given ID in the given collection
// 		refsAsBytes := b.Get([]byte(id))
// 		var refs []*IndexReference
// 		// If the response is empty return a empty list
// 		if len(refsAsBytes) == 0 {
// 			return refs
// 		}
// 		if err := json.Unmarshal(refsAsBytes, refs); err != nil {
// 			return nil
// 		}
// 		return refs
// 	}
//
// 	// // Defines the nested bucket inside MetaDatas bucket.
// 	// InternalMetaDataBuckectCollections          = InternalBuckectCollections
// 	// InternalMetaDataCollectionBuckectReferences = []byte("_refs")
// 	//
// 	// // Defines the IDs used to get internal values from the store
// 	// InternalMetaDataCollectionsID       = InternalBuckectCollections
// 	InternalMetaDataCollectionsIDSuffix = ".collection"
)

// This varable defines the deferent an error string
const (
	NotFoundString = "not found"
)
