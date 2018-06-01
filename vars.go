package GoTinyDB

import (
	"os"
)

// The values
const (
	BlockSize      = 1024 * 1024 * 10 // 10MB
	FilePermission = 0740             // u -> rwx | g -> r-- | o -> ---
	TreeOrder      = 10

	OpenDBFlags = os.O_WRONLY | os.O_CREATE | os.O_EXCL

	GetFlags = os.O_RDONLY
	PutFlags = os.O_RDWR | os.O_CREATE | os.O_TRUNC
)

// Internal buckets
var (
	InternalBuckectMetaDatas   = []byte("_metas")
	InternalBuckectCollections = []byte("_collections")
)

// Defines the nested bucket inside MetaDatas bucket.
var (
	InternalMetaDataBuckectCollections = InternalBuckectCollections
	// InternalMetaDataBuckectIndexes     = []byte("_indexes")
)

// Defines the IDs used to get internal values from the store
var (
	InternalMetaDataCollectionsID = []byte("collections")

	InternalMetaDataCollectionsIDSuffix = ".collection"
)
