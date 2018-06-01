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

// Internal common buckets and IDs
var (
	// Root buckets
	InternalBuckectMetaDatas   = []byte("_metas")
	InternalBuckectCollections = []byte("_collections")

	// Defines the nested bucket inside MetaDatas bucket.
	InternalMetaDataBuckectCollections = InternalBuckectCollections

	// Defines the IDs used to get internal values from the store
	InternalMetaDataCollectionsID       = InternalBuckectCollections
	InternalMetaDataCollectionsIDSuffix = ".collection"
)

// This varable defines the deferent an error string
const (
	NotFoundString = "not found"
)
